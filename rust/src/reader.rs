// Skeleton for the Rust-side BigQuery Storage Read POC.
//
// Wires together gRPC + auth + Arrow IPC decode. Exposed to Python as the
// `_rust.PyReader` class. Iteration yields groups of decoded
// `pyarrow.RecordBatch` objects drained from the Rust channel.

use std::sync::Arc;

use arrow::buffer::Buffer as ArrowBuffer;
use arrow::ipc::reader::StreamDecoder;
use arrow::pyarrow::ToPyArrow;
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use futures::StreamExt;
use pyo3::exceptions::{PyRuntimeError, PyStopIteration};
use pyo3::prelude::*;
use pyo3::types::PyList;
use tokio::runtime::{Builder, Runtime};
use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};
use tonic::metadata::MetadataValue;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use tonic::Request;

use crate::proto::bqstorage_v1::{
    arrow_serialization_options::CompressionCodec,
    big_query_read_client::BigQueryReadClient,
    read_session::{
        table_read_options::OutputFormatSerializationOptions, Schema as ReadSchema,
        TableModifiers, TableReadOptions,
    },
    ArrowSerializationOptions, CreateReadSessionRequest, DataFormat, ReadRowsRequest,
    ReadSession,
};

const BQ_STORAGE_ENDPOINT: &str = "https://bigquerystorage.googleapis.com";
const SCOPE: &[&str] = &["https://www.googleapis.com/auth/bigquery.readonly"];
const MAX_DECODING_MESSAGE_SIZE: usize = 256 * 1024 * 1024;
const MAX_READ_ROWS_RETRIES: usize = 3;

fn build_runtime() -> Result<Runtime, String> {
    Builder::new_multi_thread()
        .enable_all()
        .thread_name("pyarrow-bigquery-rs")
        .build()
        .map_err(|e| format!("tokio runtime: {e}"))
}

#[derive(Clone)]
struct AuthInterceptor {
    token: Arc<str>,
}

impl tonic::service::Interceptor for AuthInterceptor {
    fn call(&mut self, mut req: Request<()>) -> Result<Request<()>, tonic::Status> {
        let value: MetadataValue<_> = format!("Bearer {}", self.token)
            .parse()
            .map_err(|e| tonic::Status::internal(format!("bad auth header: {e}")))?;
        req.metadata_mut().insert("authorization", value);
        Ok(req)
    }
}

type AuthedClient =
    BigQueryReadClient<InterceptedService<Channel, AuthInterceptor>>;

async fn fetch_token() -> Result<Arc<str>, String> {
    let provider = gcp_auth::provider()
        .await
        .map_err(|e| format!("gcp auth: {e}"))?;
    let token = provider
        .token(SCOPE)
        .await
        .map_err(|e| format!("token fetch: {e}"))?;
    Ok(Arc::from(token.as_str()))
}

async fn build_client_with_token(token_str: Arc<str>) -> Result<AuthedClient, String> {
    let tls = ClientTlsConfig::new().with_native_roots();
    let channel = Endpoint::from_static(BQ_STORAGE_ENDPOINT)
        .tls_config(tls)
        .map_err(|e| format!("tls: {e}"))?
        .connect()
        .await
        .map_err(|e| format!("connect: {e}"))?;

    Ok(BigQueryReadClient::with_interceptor(
        channel,
        AuthInterceptor { token: token_str },
    )
    .max_decoding_message_size(MAX_DECODING_MESSAGE_SIZE))
}

fn compression_from_str(s: Option<&str>) -> CompressionCodec {
    match s {
        Some("lz4") => CompressionCodec::Lz4Frame,
        Some("zstd") => CompressionCodec::Zstd,
        _ => CompressionCodec::CompressionUnspecified,
    }
}

async fn create_session(
    client: &mut AuthedClient,
    parent_project: &str,
    table_resource: &str,
    selected_fields: Option<Vec<String>>,
    row_restriction: Option<String>,
    max_stream_count: i32,
    compression: CompressionCodec,
) -> Result<ReadSession, String> {
    let read_options = TableReadOptions {
        selected_fields: selected_fields.unwrap_or_default(),
        row_restriction: row_restriction.unwrap_or_default(),
        output_format_serialization_options: Some(
            OutputFormatSerializationOptions::ArrowSerializationOptions(
                ArrowSerializationOptions {
                    buffer_compression: compression as i32,
                    picos_timestamp_precision: 0,
                },
            ),
        ),
        ..Default::default()
    };

    let session = ReadSession {
        table: table_resource.to_string(),
        data_format: DataFormat::Arrow as i32,
        read_options: Some(read_options),
        table_modifiers: None::<TableModifiers>,
        ..Default::default()
    };

    let req = CreateReadSessionRequest {
        parent: format!("projects/{parent_project}"),
        read_session: Some(session),
        max_stream_count,
        preferred_min_stream_count: 0,
    };

    let resp = client
        .create_read_session(req)
        .await
        .map_err(|e| format!("CreateReadSession: {e}"))?;
    Ok(resp.into_inner())
}

async fn stream_worker(
    token: Arc<str>,
    stream_name: String,
    schema_ipc: Bytes,
    tx: mpsc::Sender<Result<RecordBatch, String>>,
) {
    // One StreamDecoder per worker: feed the schema bytes once, then each
    // gRPC message's record-batch bytes. Avoids the per-message allocate +
    // schema-prepend + StreamReader-construction overhead that dominated at
    // ~1 batch/MB of data for large reads.
    let mut decoder = StreamDecoder::new();
    let schema_slice: &[u8] = schema_ipc.as_ref();
    let mut schema_buf = ArrowBuffer::from(schema_slice);
    if let Err(e) = decoder.decode(&mut schema_buf) {
        let _ = tx
            .send(Err(format!("arrow ipc schema decode: {e}")))
            .await;
        return;
    }

    let mut offset = 0_i64;
    let mut retry_count = 0_usize;

    'read_attempt: loop {
        let mut client = match build_client_with_token(token.clone()).await {
            Ok(client) => client,
            Err(e) => {
                let _ = tx.send(Err(format!("client: {e}"))).await;
                return;
            }
        };

        let req = ReadRowsRequest {
            read_stream: stream_name.clone(),
            offset,
        };
        let mut stream = match client.read_rows(req).await {
            Ok(s) => s.into_inner(),
            Err(e) => {
                if retry_count < MAX_READ_ROWS_RETRIES {
                    retry_count += 1;
                    sleep(Duration::from_millis(250 * retry_count as u64)).await;
                    continue 'read_attempt;
                }

                let _ = tx.send(Err(format!("ReadRows: {e}"))).await;
                return;
            }
        };

        while let Some(msg) = stream.next().await {
            let response = match msg {
                Ok(r) => {
                    retry_count = 0;
                    r
                }
                Err(e) => {
                    if retry_count < MAX_READ_ROWS_RETRIES {
                        retry_count += 1;
                        sleep(Duration::from_millis(250 * retry_count as u64)).await;
                        continue 'read_attempt;
                    }

                    let _ = tx.send(Err(format!("ReadRows stream: {e}"))).await;
                    return;
                }
            };
            let Some(rows) = response.rows else { continue };
            use crate::proto::bqstorage_v1::read_rows_response::Rows;
            let Rows::ArrowRecordBatch(batch_msg) = rows else { continue };

            let batch_slice: &[u8] = batch_msg.serialized_record_batch.as_ref();
            let mut buf = ArrowBuffer::from(batch_slice);
            while !buf.is_empty() {
                match decoder.decode(&mut buf) {
                    Ok(Some(batch)) => {
                        offset += batch.num_rows() as i64;

                        if tx.send(Ok(batch)).await.is_err() {
                            return;
                        }
                    }
                    Ok(None) => break,
                    Err(e) => {
                        let _ = tx.send(Err(format!("arrow ipc decode: {e}"))).await;
                        return;
                    }
                }
            }
        }

        break;
    }
}

#[pyclass(module = "pyarrow.bigquery._rust", unsendable)]
pub struct PyReader {
    rx: Option<mpsc::Receiver<Result<RecordBatch, String>>>,
    batch_size: usize,
    schema_ipc: Vec<u8>,
    _runtime: Runtime,
}

#[pymethods]
impl PyReader {
    /// Open a BigQuery Storage read session and start streaming.
    ///
    /// Arguments mirror the Python `reader` constructor where applicable.
    /// `source` is the fully-qualified `project.dataset.table` string and
    /// `project` is the billing project used for the read session.
    #[new]
    #[pyo3(signature = (
        source,
        project,
        *,
        columns=None,
        row_restrictions=None,
        max_stream_count=10,
        compression=None,
        batch_size=100,
        channel_capacity=64,
    ))]
    fn new(
        py: Python<'_>,
        source: String,
        project: String,
        columns: Option<Vec<String>>,
        row_restrictions: Option<String>,
        max_stream_count: i32,
        compression: Option<String>,
        batch_size: usize,
        channel_capacity: usize,
    ) -> PyResult<Self> {
        let parts: Vec<&str> = source.splitn(3, '.').collect();
        if parts.len() != 3 {
            return Err(PyRuntimeError::new_err(format!(
                "source must be 'project.dataset.table', got '{source}'"
            )));
        }
        let table_resource = format!(
            "projects/{}/datasets/{}/tables/{}",
            parts[0], parts[1], parts[2]
        );
        let compression = compression_from_str(compression.as_deref());

        let rt = build_runtime().map_err(PyRuntimeError::new_err)?;
        let (schema_ipc, streams, token) = py.allow_threads(|| {
            rt.block_on(async {
                let token = fetch_token().await?;
                let mut client = build_client_with_token(token.clone()).await?;
                let session = create_session(
                    &mut client,
                    &project,
                    &table_resource,
                    columns,
                    row_restrictions,
                    max_stream_count,
                    compression,
                )
                .await?;
                let schema_ipc = match session.schema {
                    Some(ReadSchema::ArrowSchema(s)) => s.serialized_schema,
                    _ => {
                        return Err::<_, String>(
                            "BQ Storage returned non-Arrow schema".into(),
                        )
                    }
                };
                let streams: Vec<String> =
                    session.streams.into_iter().map(|s| s.name).collect();
                Ok::<_, String>((schema_ipc, streams, token))
            })
        })
        .map_err(PyRuntimeError::new_err)?;

        if streams.is_empty() {
            return Err(PyRuntimeError::new_err(
                "Read session returned no streams (table empty?)",
            ));
        }

        let (tx, rx) = mpsc::channel::<Result<RecordBatch, String>>(channel_capacity);
        let schema_bytes = Bytes::from(schema_ipc.clone());
        for stream_name in streams {
            let tx = tx.clone();
            let schema = schema_bytes.clone();
            let token = token.clone();
            rt.spawn(stream_worker(token, stream_name, schema, tx));
        }
        drop(tx);

        Ok(PyReader {
            rx: Some(rx),
            batch_size,
            schema_ipc,
            _runtime: rt,
        })
    }

    /// Serialized Arrow IPC schema bytes (the read session's schema).
    fn schema_ipc<'py>(&self, py: Python<'py>) -> Bound<'py, pyo3::types::PyBytes> {
        pyo3::types::PyBytes::new_bound(py, &self.schema_ipc)
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Pop the next decoded `pyarrow.RecordBatch` group, blocking until one is
    /// available or all workers have finished.
    fn __next__(&mut self, py: Python<'_>) -> PyResult<PyObject> {
        let Some(rx) = self.rx.as_mut() else {
            return Err(PyStopIteration::new_err(()));
        };
        let item = py.allow_threads(|| rx.blocking_recv());
        match item {
            None => {
                self.rx = None;
                Err(PyStopIteration::new_err(()))
            }
            Some(Err(e)) => Err(PyRuntimeError::new_err(e)),
            Some(Ok(batch)) => {
                let mut batches = vec![batch];
                let mut rows = batches[0].num_rows();
                while rows < self.batch_size {
                    match rx.try_recv() {
                        Ok(Ok(batch)) => {
                            rows += batch.num_rows();
                            batches.push(batch);
                        }
                        Ok(Err(e)) => return Err(PyRuntimeError::new_err(e)),
                        Err(mpsc::error::TryRecvError::Empty) => break,
                        Err(mpsc::error::TryRecvError::Disconnected) => break,
                    }
                }

                let list = PyList::empty_bound(py);
                for batch in batches {
                    list.append(batch.to_pyarrow(py)?)?;
                }
                Ok(list.into())
            }
        }
    }
}

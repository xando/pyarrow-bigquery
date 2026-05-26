// Skeleton for the Rust-side BigQuery Storage Read POC.
//
// Wires together gRPC + auth + Arrow IPC decode. Exposed to Python as the
// `_rust.PyReader` class. Iteration yields one `pyarrow.RecordBatch` per
// decoded message from the stream — the Python layer is responsible for any
// rebatching to a target row count.

use std::sync::Arc;
use std::sync::OnceLock;

use arrow::ipc::reader::StreamReader as ArrowStreamReader;
use arrow::pyarrow::ToPyArrow;
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use futures::StreamExt;
use pyo3::exceptions::{PyRuntimeError, PyStopIteration};
use pyo3::prelude::*;
use tokio::runtime::{Builder, Runtime};
use tokio::sync::mpsc;
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

fn runtime() -> &'static Runtime {
    static RT: OnceLock<Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        Builder::new_multi_thread()
            .enable_all()
            .thread_name("pyarrow-bigquery-rs")
            .build()
            .expect("failed to build tokio runtime")
    })
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

async fn build_client() -> Result<AuthedClient, String> {
    let provider = gcp_auth::provider()
        .await
        .map_err(|e| format!("gcp auth: {e}"))?;
    let token = provider
        .token(SCOPE)
        .await
        .map_err(|e| format!("token fetch: {e}"))?;
    let token_str: Arc<str> = Arc::from(token.as_str());

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
    ))
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
    mut client: AuthedClient,
    stream_name: String,
    schema_ipc: Bytes,
    tx: mpsc::Sender<Result<RecordBatch, String>>,
) {
    let req = ReadRowsRequest {
        read_stream: stream_name,
        offset: 0,
    };
    let mut stream = match client.read_rows(req).await {
        Ok(s) => s.into_inner(),
        Err(e) => {
            let _ = tx.send(Err(format!("ReadRows: {e}"))).await;
            return;
        }
    };

    while let Some(msg) = stream.next().await {
        match msg {
            Ok(response) => {
                let Some(rows) = response.rows else { continue };
                use crate::proto::bqstorage_v1::read_rows_response::Rows;
                let Rows::ArrowRecordBatch(batch_msg) = rows else { continue };
                // Concatenate schema + batch bytes into an Arrow IPC stream so
                // we can use StreamReader (which understands continuation/EOS).
                let mut buf = Vec::with_capacity(
                    schema_ipc.len() + batch_msg.serialized_record_batch.len(),
                );
                buf.extend_from_slice(&schema_ipc);
                buf.extend_from_slice(&batch_msg.serialized_record_batch);
                let cursor = std::io::Cursor::new(buf);
                let reader = match ArrowStreamReader::try_new(cursor, None) {
                    Ok(r) => r,
                    Err(e) => {
                        let _ = tx.send(Err(format!("arrow ipc open: {e}"))).await;
                        return;
                    }
                };
                for batch in reader {
                    let batch = match batch {
                        Ok(b) => b,
                        Err(e) => {
                            let _ = tx.send(Err(format!("arrow ipc decode: {e}"))).await;
                            return;
                        }
                    };
                    if tx.send(Ok(batch)).await.is_err() {
                        return;
                    }
                }
            }
            Err(e) => {
                let _ = tx.send(Err(format!("ReadRows stream: {e}"))).await;
                return;
            }
        }
    }
}

#[pyclass(module = "pyarrow.bigquery._rust", unsendable)]
pub struct PyReader {
    rx: Option<mpsc::Receiver<Result<RecordBatch, String>>>,
    schema_ipc: Vec<u8>,
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

        let rt = runtime();
        let (schema_ipc, streams, client) = py.allow_threads(|| {
            rt.block_on(async {
                let mut client = build_client().await?;
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
                Ok::<_, String>((schema_ipc, streams, client))
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
            let client = client.clone();
            rt.spawn(stream_worker(client, stream_name, schema, tx));
        }
        drop(tx);

        Ok(PyReader {
            rx: Some(rx),
            schema_ipc,
        })
    }

    /// Serialized Arrow IPC schema bytes (the read session's schema).
    fn schema_ipc<'py>(&self, py: Python<'py>) -> Bound<'py, pyo3::types::PyBytes> {
        pyo3::types::PyBytes::new_bound(py, &self.schema_ipc)
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Pop the next decoded `pyarrow.RecordBatch`, blocking until one is
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
            Some(Ok(batch)) => batch.to_pyarrow(py),
        }
    }
}

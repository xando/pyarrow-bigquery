//! Arrow Storage Write transport used by the Python writer's Rust engine.

// PyO3 0.22 generates redundant PyErr conversions outside the annotated
// methods, so this lint must be allowed at the containing module level.
#![allow(clippy::useless_conversion)]

use std::collections::VecDeque;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};

use arrow::datatypes::{Schema, SchemaRef};
use arrow::ipc::writer::{write_message, DictionaryTracker, IpcDataGenerator, IpcWriteOptions};
use arrow::ipc::CompressionType;
use arrow::pyarrow::FromPyArrow;
use arrow::record_batch::RecordBatch;
use futures::{Stream, StreamExt};
use prost::Message;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use tokio::runtime::{Builder, Runtime};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tonic::codec::CompressionEncoding;
use tonic::metadata::MetadataValue;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use tonic::Request;

use crate::proto::bqstorage_v1::{
    append_rows_request::{ArrowData, Rows},
    append_rows_response::Response,
    big_query_write_client::BigQueryWriteClient,
    write_stream::Type as StreamType,
    AppendRowsRequest, AppendRowsResponse, ArrowRecordBatch, ArrowSchema,
    BatchCommitWriteStreamsRequest, BatchCommitWriteStreamsResponse, CreateWriteStreamRequest,
    FinalizeWriteStreamRequest, WriteStream,
};
use crate::writer_schema::{normalize_batch, normalize_schema};

const ENDPOINT: &str = "https://bigquerystorage.googleapis.com";
const SCOPE: &[&str] = &["https://www.googleapis.com/auth/bigquery"];
// The Storage Write protocol requires each complete protobuf request to be <10 MB.
const MAX_REQUEST_BYTES: usize = 10_000_000;
const WORKER_QUEUE_CAPACITY: usize = 2;
// Bound both unacknowledged appends and their serialized buffers per stream.
const MAX_IN_FLIGHT_REQUESTS: usize = 4;
const MAX_CREATE_RETRIES: usize = 5;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum WriteCompression {
    #[default]
    Uncompressed,
    Gzip,
    Lz4,
    Zstd,
}

impl WriteCompression {
    fn transport(self) -> Option<CompressionEncoding> {
        match self {
            Self::Gzip => Some(CompressionEncoding::Gzip),
            _ => None,
        }
    }

    fn arrow(self) -> Option<CompressionType> {
        match self {
            Self::Lz4 => Some(CompressionType::LZ4_FRAME),
            Self::Zstd => Some(CompressionType::ZSTD),
            _ => None,
        }
    }
}

fn compression_from_str(compression: Option<&str>) -> Result<WriteCompression, String> {
    match compression {
        None => Ok(WriteCompression::Uncompressed),
        Some("gzip") => Ok(WriteCompression::Gzip),
        Some("lz4") => Ok(WriteCompression::Lz4),
        Some("zstd") => Ok(WriteCompression::Zstd),
        Some(value) => Err(format!(
            "Unsupported write compression {value:?}, expected None, 'gzip', 'lz4' or 'zstd'"
        )),
    }
}

#[derive(Clone)]
struct AuthInterceptor {
    token: Arc<str>,
}

impl tonic::service::Interceptor for AuthInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, tonic::Status> {
        let value: MetadataValue<_> = format!("Bearer {}", self.token)
            .parse()
            .map_err(|e| tonic::Status::internal(format!("bad auth header: {e}")))?;
        request.metadata_mut().insert("authorization", value);
        Ok(request)
    }
}

type Client = BigQueryWriteClient<InterceptedService<Channel, AuthInterceptor>>;

async fn build_client(compression: Option<CompressionEncoding>) -> Result<Client, String> {
    let provider = gcp_auth::provider()
        .await
        .map_err(|e| format!("gcp auth: {e}"))?;
    let token = provider
        .token(SCOPE)
        .await
        .map_err(|e| format!("token fetch: {e}"))?;
    let channel = Endpoint::from_static(ENDPOINT)
        .tls_config(ClientTlsConfig::new().with_native_roots())
        .map_err(|e| format!("tls: {e}"))?
        .connect()
        .await
        .map_err(|e| format!("connect: {e}"))?;
    let client = BigQueryWriteClient::with_interceptor(
        channel,
        AuthInterceptor {
            token: Arc::from(token.as_str()),
        },
    )
    .max_encoding_message_size(MAX_REQUEST_BYTES)
    .max_decoding_message_size(MAX_REQUEST_BYTES);
    Ok(match compression {
        Some(encoding) => client.send_compressed(encoding),
        None => client,
    })
}

fn routed_request<T>(body: T, key: &str, value: &str) -> Result<Request<T>, String> {
    let mut request = Request::new(body);
    request.metadata_mut().insert(
        "x-goog-request-params",
        format!("{key}={value}")
            .parse()
            .map_err(|e| format!("invalid resource name: {e}"))?,
    );
    Ok(request)
}

#[derive(Default)]
struct Failure(Mutex<Option<String>>);

impl Failure {
    fn get(&self) -> Option<String> {
        self.0.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }

    fn record(&self, error: String) {
        let mut first = self.0.lock().unwrap_or_else(|e| e.into_inner());
        if first.is_none() {
            *first = Some(error);
        }
    }

    fn result(&self) -> Result<(), String> {
        self.get().map_or(Ok(()), Err)
    }
}

fn schema_ipc(schema: &Schema) -> Result<Vec<u8>, String> {
    let options = IpcWriteOptions::default();
    let encoded = IpcDataGenerator::default().schema_to_bytes_with_dictionary_tracker(
        schema,
        &mut DictionaryTracker::new(false),
        &options,
    );
    let mut bytes = Vec::new();
    write_message(&mut bytes, encoded, &options).map_err(|e| format!("Arrow schema: {e}"))?;
    Ok(bytes)
}

fn batch_ipc(batch: &RecordBatch, compression: WriteCompression) -> Result<Vec<u8>, String> {
    let options = IpcWriteOptions::default()
        .try_with_compression(compression.arrow())
        .map_err(|e| format!("Arrow compression: {e}"))?;
    let (dictionaries, encoded) = IpcDataGenerator::default()
        .encoded_batch(batch, &mut DictionaryTracker::new(false), &options)
        .map_err(|e| format!("Arrow batch: {e}"))?;
    if !dictionaries.is_empty() {
        return Err("dictionary arrays must be normalized before writing".into());
    }
    let mut bytes = Vec::new();
    write_message(&mut bytes, encoded, &options).map_err(|e| format!("Arrow batch: {e}"))?;
    Ok(bytes)
}

fn append_request(
    stream: &str,
    offset: i64,
    schema: Option<&[u8]>,
    batch: &RecordBatch,
    compression: WriteCompression,
) -> Result<AppendRowsRequest, String> {
    Ok(AppendRowsRequest {
        write_stream: stream.to_owned(),
        offset: Some(offset),
        rows: Some(Rows::ArrowRows(ArrowData {
            writer_schema: schema.map(|bytes| ArrowSchema {
                serialized_schema: bytes.to_vec(),
            }),
            rows: Some(ArrowRecordBatch {
                serialized_record_batch: batch_ipc(batch, compression)?,
                ..Default::default()
            }),
        })),
        ..Default::default()
    })
}

// Return the largest tried prefix that fits. The check covers the actual
// protobuf, including its stream name, offset and (on the first request) schema.
fn fitting_request(
    stream: &str,
    offset: i64,
    schema: Option<&[u8]>,
    batch: &RecordBatch,
    limit: usize,
    compression: WriteCompression,
) -> Result<(AppendRowsRequest, usize), String> {
    let mut rows = batch.num_rows();
    if rows == 0 {
        return Err("cannot append an empty record batch".into());
    }
    loop {
        let request = append_request(stream, offset, schema, &batch.slice(0, rows), compression)?;
        if request.encoded_len() < limit {
            return Ok((request, rows));
        }
        if rows == 1 {
            return Err(format!(
                "a single Arrow row with its schema exceeds the {limit}-byte append request limit"
            ));
        }
        rows /= 2;
    }
}

fn check_append(response: AppendRowsResponse, expected_offset: i64) -> Result<(), String> {
    if !response.row_errors.is_empty() {
        return Err(format!("AppendRows row errors: {:?}", response.row_errors));
    }
    match response.response {
        Some(Response::AppendResult(result)) if result.offset == Some(expected_offset) => Ok(()),
        Some(Response::AppendResult(result)) => Err(format!(
            "AppendRows acknowledged offset {:?}, expected {expected_offset}",
            result.offset
        )),
        Some(Response::Error(error)) => Err(format!(
            "AppendRows failed (code {}): {}",
            error.code, error.message
        )),
        None => Err("AppendRows returned no append result".into()),
    }
}

fn check_commit(response: BatchCommitWriteStreamsResponse) -> Result<(), String> {
    if !response.stream_errors.is_empty() {
        return Err(format!(
            "BatchCommitWriteStreams failed: {:?}",
            response.stream_errors
        ));
    }
    if response.commit_time.is_none() {
        return Err("BatchCommitWriteStreams returned no commit time".into());
    }
    Ok(())
}

struct AppendConnection<S = tonic::Streaming<AppendRowsResponse>> {
    requests: mpsc::Sender<AppendRowsRequest>,
    responses: S,
    pending: VecDeque<i64>,
    error: Option<String>,
}

impl AppendConnection {
    async fn open(client: &mut Client, first: AppendRowsRequest) -> Result<Self, String> {
        let stream_name = first.write_stream.clone();
        let offset = first
            .offset
            .ok_or("AppendRows request is missing its offset")?;
        let (tx, rx) = mpsc::channel(1);
        // Queue the first request before opening: the server can wait for its
        // stream name and schema before sending the response headers.
        tx.send(first)
            .await
            .map_err(|_| "AppendRows request channel closed".to_string())?;
        let requests = futures::stream::unfold(rx, |mut rx| async {
            rx.recv().await.map(|request| (request, rx))
        });
        let responses = client
            .append_rows(routed_request(requests, "write_stream", &stream_name)?)
            .await
            .map_err(|e| format!("AppendRows: {e}"))?
            .into_inner();
        Ok(Self {
            requests: tx,
            responses,
            pending: VecDeque::from([offset]),
            error: None,
        })
    }
}

impl<S> AppendConnection<S>
where
    S: Stream<Item = Result<AppendRowsResponse, tonic::Status>> + Unpin,
{
    fn fail(&mut self, error: String) -> String {
        self.error.get_or_insert(error).clone()
    }

    async fn send(&mut self, request: AppendRowsRequest) -> Result<(), String> {
        if let Some(error) = &self.error {
            return Err(error.clone());
        }
        if self.pending.len() == MAX_IN_FLIGHT_REQUESTS {
            self.acknowledge_next().await?;
        }
        let offset = request
            .offset
            .ok_or("AppendRows request is missing its offset")?;
        self.requests
            .send(request)
            .await
            .map_err(|_| self.fail("AppendRows request channel closed".into()))?;
        self.pending.push_back(offset);
        Ok(())
    }

    async fn acknowledge_next(&mut self) -> Result<(), String> {
        let offset = self
            .pending
            .pop_front()
            .ok_or("No pending AppendRows request")?;
        let result = match self.responses.next().await {
            Some(Ok(response)) => check_append(response, offset),
            Some(Err(error)) => Err(format!("AppendRows response: {error}")),
            None => Err("AppendRows closed before acknowledging a request".into()),
        };
        result.map_err(|error| self.fail(error))
    }

    async fn finish(self) -> Result<(), String> {
        let Self {
            requests,
            mut responses,
            mut pending,
            mut error,
        } = self;
        // Half-close first, then drain all tail acknowledgements and the
        // terminal status. Keep the first error, even if later appends succeed.
        drop(requests);
        while let Some(response) = responses.next().await {
            let result = match response {
                Ok(response) => match pending.pop_front() {
                    Some(offset) => check_append(response, offset),
                    None => Err("AppendRows returned an unexpected extra response".into()),
                },
                Err(status) => {
                    error.get_or_insert_with(|| format!("AppendRows completion: {status}"));
                    break;
                }
            };
            if let Err(append_error) = result {
                error.get_or_insert(append_error);
            }
        }
        if !pending.is_empty() {
            error.get_or_insert("AppendRows closed before acknowledging a request".into());
        }
        error.map_or(Ok(()), Err)
    }
}

async fn create_stream(
    parent: &str,
    compression: Option<CompressionEncoding>,
) -> Result<(Client, String), String> {
    let mut client = build_client(compression).await?;
    let mut attempt = 0;
    let response = loop {
        let result = client
            .create_write_stream(routed_request(
                CreateWriteStreamRequest {
                    parent: parent.to_owned(),
                    write_stream: Some(WriteStream {
                        r#type: StreamType::Pending as i32,
                        ..Default::default()
                    }),
                },
                "parent",
                parent,
            )?)
            .await;
        match result {
            Ok(response) => break response.into_inner(),
            Err(error)
                if attempt < MAX_CREATE_RETRIES
                    && matches!(error.code(), tonic::Code::NotFound | tonic::Code::Unknown) =>
            {
                // A table created through the REST API may not yet be visible
                // to Storage Write. No rows have been sent at this point, so
                // retrying stream creation cannot duplicate any data.
                tokio::time::sleep(std::time::Duration::from_millis(250 * (1 << attempt))).await;
                attempt += 1;
            }
            Err(error) => {
                return Err(format!(
                    "CreateWriteStream failed after {} attempt(s): {error}",
                    attempt + 1
                ))
            }
        }
    };
    if response.name.is_empty() {
        return Err("CreateWriteStream returned an empty stream name".into());
    }
    Ok((client, response.name))
}

async fn finalize_stream(name: &str, expected_rows: i64) -> Result<(), String> {
    // Fetch fresh credentials: a writer may have remained open longer than
    // the token used to establish its AppendRows connection.
    let mut client = build_client(None).await?;
    let response = client
        .finalize_write_stream(routed_request(
            FinalizeWriteStreamRequest { name: name.into() },
            "name",
            name,
        )?)
        .await
        .map_err(|e| format!("FinalizeWriteStream: {e}"))?
        .into_inner();
    if response.row_count != expected_rows {
        return Err(format!(
            "FinalizeWriteStream returned {} rows, expected {expected_rows}",
            response.row_count
        ));
    }
    Ok(())
}

async fn commit_streams(parent: &str, streams: Vec<String>) -> Result<(), String> {
    let mut client = build_client(None).await?;
    let response = client
        .batch_commit_write_streams(routed_request(
            BatchCommitWriteStreamsRequest {
                parent: parent.into(),
                write_streams: streams,
            },
            "parent",
            parent,
        )?)
        .await
        .map_err(|e| format!("BatchCommitWriteStreams: {e}"))?
        .into_inner();
    check_commit(response)
}

async fn worker(
    parent: String,
    schema: Arc<Vec<u8>>,
    compression: WriteCompression,
    mut batches: mpsc::Receiver<RecordBatch>,
    failure: Arc<Failure>,
    abort: Arc<AtomicBool>,
) -> Option<String> {
    let mut client_and_name: Option<(Client, String)> = None;
    let mut connection: Option<AppendConnection> = None;
    let mut offset = 0_i64;
    while let Some(batch) = batches.recv().await {
        // Drain queued work after an error/abort, so blocked producers and
        // other workers can finish. Nothing is committed in either case.
        if abort.load(Ordering::Acquire) || failure.get().is_some() {
            continue;
        }
        if client_and_name.is_none() {
            match create_stream(&parent, compression.transport()).await {
                Ok(stream) => client_and_name = Some(stream),
                Err(error) => {
                    failure.record(error);
                    continue;
                }
            }
        }
        let (client, name) = client_and_name.as_mut().expect("stream was initialized");
        let mut start = 0;
        while start < batch.num_rows() {
            if abort.load(Ordering::Acquire) || failure.get().is_some() {
                break;
            }
            let piece = batch.slice(start, batch.num_rows() - start);
            let result = async {
                let (request, rows) = fitting_request(
                    name,
                    offset,
                    connection.is_none().then_some(schema.as_slice()),
                    &piece,
                    MAX_REQUEST_BYTES,
                    compression,
                )?;
                if let Some(connection) = connection.as_mut() {
                    connection.send(request).await?;
                } else {
                    connection = Some(AppendConnection::open(client, request).await?);
                }
                // Advance the sent offset as soon as the request is queued.
                // AppendConnection checks ordered acknowledgements separately;
                // finish() must drain them all before any stream can commit.
                let rows_i64 = i64::try_from(rows).map_err(|_| "row count overflow".to_string())?;
                offset = offset
                    .checked_add(rows_i64)
                    .ok_or_else(|| "write offset overflow".to_string())?;
                Ok::<usize, String>(rows)
            }
            .await;
            match result {
                Ok(rows) => start += rows,
                Err(error) => {
                    failure.record(error);
                    break;
                }
            }
        }
    }
    if let Some(connection) = connection {
        if let Err(error) = connection.finish().await {
            failure.record(error);
        }
    }
    if let Some((_, name)) = client_and_name {
        if let Err(error) = finalize_stream(&name, offset).await {
            failure.record(error);
        }
        Some(name)
    } else {
        None
    }
}

struct Worker {
    sender: mpsc::Sender<RecordBatch>,
    task: JoinHandle<Option<String>>,
}

#[pyclass(module = "pyarrow.bigquery._rust", unsendable)]
pub struct PyWriter {
    parent: String,
    input_schema: SchemaRef,
    schema: SchemaRef,
    schema_ipc: Arc<Vec<u8>>,
    worker_count: usize,
    compression: WriteCompression,
    next_worker: usize,
    workers: Vec<Worker>,
    failure: Arc<Failure>,
    abort: Arc<AtomicBool>,
    closed: bool,
    runtime: Option<Runtime>,
}

impl PyWriter {
    fn start(&mut self) -> Result<(), String> {
        if self.runtime.is_some() {
            return Ok(());
        }
        let runtime = Builder::new_multi_thread()
            .enable_all()
            .thread_name("pyarrow-bigquery-write")
            .build()
            .map_err(|e| format!("tokio runtime: {e}"))?;
        for _ in 0..self.worker_count {
            let (sender, receiver) = mpsc::channel(WORKER_QUEUE_CAPACITY);
            let task = runtime.spawn(worker(
                self.parent.clone(),
                self.schema_ipc.clone(),
                self.compression,
                receiver,
                self.failure.clone(),
                self.abort.clone(),
            ));
            self.workers.push(Worker { sender, task });
        }
        self.runtime = Some(runtime);
        Ok(())
    }

    fn enqueue(&mut self, batch: RecordBatch) -> Result<(), String> {
        self.failure.result()?;
        if self.closed {
            return Err("writer is closed".into());
        }
        if batch.schema() != self.input_schema {
            return Err("record batch schema does not match the writer schema".into());
        }
        let batch = normalize_batch(&batch, &self.schema).map_err(|e| e.to_string())?;
        if batch.num_rows() == 0 {
            return Ok(());
        }
        self.start()?;
        self.workers[self.next_worker]
            .sender
            .blocking_send(batch)
            .map_err(|_| "BigQuery write worker stopped unexpectedly".to_string())?;
        self.next_worker = (self.next_worker + 1) % self.workers.len();
        self.failure.result()
    }

    fn finish(&mut self, commit: bool) -> Result<(), String> {
        if self.closed {
            return self.failure.result();
        }
        self.closed = true;
        self.abort.store(!commit, Ordering::Release);
        let workers = std::mem::take(&mut self.workers);
        let Some(runtime) = self.runtime.take() else {
            return self.failure.result();
        };
        let tasks: Vec<_> = workers
            .into_iter()
            .map(|worker| {
                drop(worker.sender);
                worker.task
            })
            .collect();
        runtime.block_on(async {
            let mut streams = Vec::new();
            // Join every task even when another one has failed. Each task
            // drains its outstanding acknowledgement and finalizes its stream.
            for result in futures::future::join_all(tasks).await {
                match result {
                    Ok(Some(name)) => streams.push(name),
                    Ok(None) => {}
                    Err(error) => self
                        .failure
                        .record(format!("BigQuery write worker: {error}")),
                }
            }
            if commit && self.failure.get().is_none() && !streams.is_empty() {
                if let Err(error) = commit_streams(&self.parent, streams).await {
                    self.failure.record(error);
                }
            }
        });
        self.failure.result()
    }
}

#[pymethods]
impl PyWriter {
    #[new]
    #[pyo3(signature = (parent, schema, worker_count=10, compression=None))]
    fn new(
        parent: String,
        schema: &Bound<'_, PyAny>,
        worker_count: usize,
        compression: Option<&str>,
    ) -> PyResult<Self> {
        let compression = compression_from_str(compression).map_err(PyValueError::new_err)?;
        let parts: Vec<_> = parent.split('/').collect();
        if parts.len() != 6
            || parts[0] != "projects"
            || parts[2] != "datasets"
            || parts[4] != "tables"
            || parts.iter().any(|part| part.is_empty())
        {
            return Err(PyValueError::new_err(
                "parent must be 'projects/<project>/datasets/<dataset>/tables/<table>'",
            ));
        }
        if worker_count == 0 {
            return Err(PyValueError::new_err(
                "worker_count must be greater than zero",
            ));
        }
        let input_schema = Arc::new(Schema::from_pyarrow_bound(schema)?);
        let schema = Arc::new(normalize_schema(&input_schema).map_err(|e| {
            PyValueError::new_err(format!("unsupported BigQuery Arrow schema: {e}"))
        })?);
        let schema_ipc = Arc::new(schema_ipc(&schema).map_err(PyValueError::new_err)?);
        Ok(Self {
            parent,
            input_schema,
            schema,
            schema_ipc,
            worker_count,
            compression,
            next_worker: 0,
            workers: Vec::new(),
            failure: Arc::default(),
            abort: Arc::new(AtomicBool::new(false)),
            closed: false,
            runtime: None,
        })
    }

    fn write_batch(&mut self, py: Python<'_>, batch: &Bound<'_, PyAny>) -> PyResult<()> {
        let batch = match RecordBatch::from_pyarrow_bound(batch) {
            Ok(batch) => batch,
            Err(error) => {
                self.failure.record(error.to_string());
                return Err(error);
            }
        };
        let result = py.allow_threads(|| self.enqueue(batch));
        if let Err(error) = &result {
            self.failure.record(error.clone());
        }
        result.map_err(PyRuntimeError::new_err)
    }

    #[pyo3(signature = (commit=true))]
    fn close(&mut self, py: Python<'_>, commit: bool) -> PyResult<()> {
        py.allow_threads(|| self.finish(commit))
            .map_err(PyRuntimeError::new_err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::bqstorage_v1::{append_rows_response::AppendResult, RowError, StorageError};
    use arrow::array::{Int64Array, StringArray};
    use arrow::buffer::Buffer;
    use arrow::datatypes::{DataType, Field};
    use arrow::ipc::reader::StreamDecoder;
    use tonic::codegen::tokio_stream::wrappers::ReceiverStream;

    type TestConnection =
        AppendConnection<ReceiverStream<Result<AppendRowsResponse, tonic::Status>>>;

    fn test_connection() -> (
        TestConnection,
        mpsc::Receiver<AppendRowsRequest>,
        mpsc::Sender<Result<AppendRowsResponse, tonic::Status>>,
    ) {
        let (requests, receiver) = mpsc::channel(MAX_IN_FLIGHT_REQUESTS);
        let (responses, response_receiver) = mpsc::channel(MAX_IN_FLIGHT_REQUESTS + 2);
        (
            AppendConnection {
                requests,
                responses: ReceiverStream::new(response_receiver),
                pending: VecDeque::new(),
                error: None,
            },
            receiver,
            responses,
        )
    }

    fn request_at(offset: i64) -> AppendRowsRequest {
        AppendRowsRequest {
            offset: Some(offset),
            ..Default::default()
        }
    }

    fn ack_at(offset: i64) -> AppendRowsResponse {
        AppendRowsResponse {
            response: Some(Response::AppendResult(AppendResult {
                offset: Some(offset),
            })),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn pipelines_a_bounded_window_and_checks_acknowledgements_in_order() {
        let (mut connection, mut requests, responses) = test_connection();
        for index in 0..MAX_IN_FLIGHT_REQUESTS {
            connection
                .send(request_at(index as i64 * 10))
                .await
                .unwrap();
            assert_eq!(
                requests.recv().await.unwrap().offset,
                Some(index as i64 * 10)
            );
        }
        assert_eq!(connection.pending.len(), MAX_IN_FLIGHT_REQUESTS);
        // All four appends reached the server without any acknowledgement.
        // A fifth must wait for the oldest response before it can be sent.
        let next_offset = MAX_IN_FLIGHT_REQUESTS as i64 * 10;
        let mut fifth = Box::pin(connection.send(request_at(next_offset)));
        assert!(futures::poll!(fifth.as_mut()).is_pending());
        assert!(matches!(
            requests.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        responses.send(Ok(ack_at(0))).await.unwrap();
        fifth.await.unwrap();
        assert_eq!(requests.recv().await.unwrap().offset, Some(next_offset));
        assert_eq!(connection.pending.len(), MAX_IN_FLIGHT_REQUESTS);
        for index in 1..=MAX_IN_FLIGHT_REQUESTS {
            responses.send(Ok(ack_at(index as i64 * 10))).await.unwrap();
        }
        drop(responses);
        connection.finish().await.unwrap();
        assert!(requests.recv().await.is_none());
    }

    #[tokio::test]
    async fn half_closes_before_waiting_for_tail_acknowledgements() {
        let (mut connection, mut requests, responses) = test_connection();
        let server = tokio::spawn(async move {
            let mut offsets = Vec::new();
            while let Some(request) = requests.recv().await {
                offsets.push(request.offset.unwrap());
            }
            // A server is allowed to delay its responses until half-close.
            for offset in &offsets {
                responses.send(Ok(ack_at(*offset))).await.unwrap();
            }
            offsets
        });
        for offset in [0, 10, 20] {
            connection.send(request_at(offset)).await.unwrap();
        }
        connection.finish().await.unwrap();
        assert_eq!(server.await.unwrap(), vec![0, 10, 20]);
    }

    #[tokio::test]
    async fn missing_tail_acknowledgements_prevent_success() {
        let (mut connection, _requests, responses) = test_connection();
        connection.send(request_at(0)).await.unwrap();
        connection.send(request_at(10)).await.unwrap();
        responses.send(Ok(ack_at(0))).await.unwrap();
        drop(responses);
        assert!(connection
            .finish()
            .await
            .unwrap_err()
            .contains("before acknowledging"));
    }

    #[tokio::test]
    async fn extra_responses_prevent_success() {
        let (mut connection, _requests, responses) = test_connection();
        connection.send(request_at(0)).await.unwrap();
        responses.send(Ok(ack_at(0))).await.unwrap();
        responses.send(Ok(ack_at(10))).await.unwrap();
        drop(responses);
        assert!(connection
            .finish()
            .await
            .unwrap_err()
            .contains("extra response"));
    }

    #[tokio::test]
    async fn terminal_error_after_all_acknowledgements_prevents_success() {
        let (mut connection, _requests, responses) = test_connection();
        connection.send(request_at(0)).await.unwrap();
        responses.send(Ok(ack_at(0))).await.unwrap();
        responses
            .send(Err(tonic::Status::unavailable("connection lost")))
            .await
            .unwrap();
        drop(responses);
        assert!(connection
            .finish()
            .await
            .unwrap_err()
            .contains("connection lost"));
    }

    #[tokio::test]
    async fn drains_tail_responses_after_an_error_and_preserves_the_first_error() {
        let observed = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counter = observed.clone();
        let (requests, mut receiver) = mpsc::channel(1);
        let responses = futures::stream::iter(vec![
            Ok(ack_at(99)),
            Ok(AppendRowsResponse {
                row_errors: vec![RowError::default()],
                ..ack_at(10)
            }),
            Ok(ack_at(20)),
            Err(tonic::Status::unavailable("later failure")),
        ])
        .inspect(move |_| {
            counter.fetch_add(1, Ordering::SeqCst);
        });
        let connection = AppendConnection {
            requests,
            responses,
            pending: VecDeque::from([0, 10, 20]),
            error: None,
        };
        let error = connection.finish().await.unwrap_err();
        assert!(error.contains("offset Some(99), expected 0"));
        assert_eq!(observed.load(Ordering::SeqCst), 4);
        assert!(receiver.recv().await.is_none());
    }

    #[tokio::test]
    async fn bad_window_acknowledgement_stops_new_appends() {
        let (mut connection, mut requests, responses) = test_connection();
        for index in 0..MAX_IN_FLIGHT_REQUESTS {
            connection
                .send(request_at(index as i64 * 10))
                .await
                .unwrap();
            requests.recv().await.unwrap();
        }
        responses.send(Ok(ack_at(99))).await.unwrap();
        let error = connection.send(request_at(40)).await.unwrap_err();
        assert!(error.contains("offset"));
        assert_eq!(connection.send(request_at(40)).await.unwrap_err(), error);
        assert!(matches!(
            requests.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        for index in 1..MAX_IN_FLIGHT_REQUESTS {
            responses.send(Ok(ack_at(index as i64 * 10))).await.unwrap();
        }
        drop(responses);
        assert_eq!(connection.finish().await.unwrap_err(), error);
    }

    #[tokio::test]
    async fn closed_request_channel_stops_new_appends_and_poison_is_preserved() {
        let (mut connection, requests, responses) = test_connection();
        drop(requests);
        let error = connection.send(request_at(0)).await.unwrap_err();
        assert!(error.contains("request channel closed"));
        assert!(connection.pending.is_empty());
        assert_eq!(connection.send(request_at(10)).await.unwrap_err(), error);
        drop(responses);
        assert_eq!(connection.finish().await.unwrap_err(), error);
    }

    #[test]
    fn validates_write_compression() {
        for (value, expected) in [
            (None, WriteCompression::Uncompressed),
            (Some("gzip"), WriteCompression::Gzip),
            (Some("lz4"), WriteCompression::Lz4),
            (Some("zstd"), WriteCompression::Zstd),
        ] {
            let compression = compression_from_str(value).unwrap();
            assert_eq!(compression, expected);
            assert!(compression.transport().is_none() || compression.arrow().is_none());
        }
        for value in ["snappy", "brotli", "GZIP", ""] {
            assert!(compression_from_str(Some(value))
                .unwrap_err()
                .contains("Unsupported write compression"));
        }
    }

    const COMPRESSIONS: [WriteCompression; 4] = [
        WriteCompression::Uncompressed,
        WriteCompression::Gzip,
        WriteCompression::Lz4,
        WriteCompression::Zstd,
    ];

    fn batch() -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("value", DataType::Utf8, true)])),
            vec![Arc::new(StringArray::from(vec!["a".repeat(500); 8]))],
        )
        .unwrap()
    }

    #[test]
    fn ipc_is_one_schema_and_one_batch_message() {
        let batch = batch();
        let plain_size = batch_ipc(&batch, WriteCompression::Uncompressed)
            .unwrap()
            .len();
        for compression in COMPRESSIONS {
            let mut decoder = StreamDecoder::new();
            let mut schema = Buffer::from(schema_ipc(&batch.schema()).unwrap());
            assert!(decoder.decode(&mut schema).unwrap().is_none());
            assert!(schema.is_empty());
            let bytes = batch_ipc(&batch, compression).unwrap();
            let metadata = arrow::ipc::root_as_message(&bytes[8..]).unwrap();
            assert_eq!(
                metadata
                    .header_as_record_batch()
                    .unwrap()
                    .compression()
                    .map(|c| c.codec()),
                compression.arrow()
            );
            if compression.arrow().is_some() {
                assert!(bytes.len() < plain_size);
            }
            let mut encoded = Buffer::from(bytes);
            assert_eq!(decoder.decode(&mut encoded).unwrap().unwrap(), batch);
            assert!(encoded.is_empty());
        }
    }

    #[test]
    fn splits_using_complete_request_size_and_preserves_rows() {
        // Distinct data keeps the full batch larger than a single row for all
        // codecs. Highly repetitive data can have non-monotonic encoded sizes.
        let mut state = 17_u64;
        let values: Vec<String> = (0..8)
            .map(|_| {
                (0..500)
                    .map(|_| {
                        state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
                        char::from(32 + ((state >> 32) % 95) as u8)
                    })
                    .collect()
            })
            .collect();
        let batch =
            RecordBatch::try_new(batch().schema(), vec![Arc::new(StringArray::from(values))])
                .unwrap();
        let schema = schema_ipc(&batch.schema()).unwrap();
        let stream = "projects/p/datasets/d/tables/t/streams/12345";
        for compression in COMPRESSIONS {
            // Force the complete batch to split, using the actual compressed size.
            let limit = append_request(stream, 0, Some(&schema), &batch, compression)
                .unwrap()
                .encoded_len();
            let mut start = 0;
            let mut recovered = Vec::new();
            while start < batch.num_rows() {
                let (request, rows) = fitting_request(
                    stream,
                    start as i64,
                    (start == 0).then_some(schema.as_slice()),
                    &batch.slice(start, batch.num_rows() - start),
                    limit,
                    compression,
                )
                .unwrap();
                assert!(request.encoded_len() < limit);
                assert_eq!(request.offset, Some(start as i64));
                let Some(Rows::ArrowRows(data)) = request.rows else {
                    panic!("not Arrow")
                };
                assert_eq!(data.writer_schema.is_some(), start == 0);
                let mut decoder = StreamDecoder::new();
                decoder.decode(&mut Buffer::from(schema.clone())).unwrap();
                recovered.push(
                    decoder
                        .decode(&mut Buffer::from(
                            data.rows.unwrap().serialized_record_batch,
                        ))
                        .unwrap()
                        .unwrap(),
                );
                start += rows;
            }
            assert!(recovered.len() > 1);
            assert_eq!(
                arrow::compute::concat_batches(&batch.schema(), &recovered).unwrap(),
                batch
            );
        }
    }

    #[test]
    fn rejects_oversized_single_row_including_schema() {
        let batch = batch().slice(0, 1);
        let schema = schema_ipc(&batch.schema()).unwrap();
        for compression in COMPRESSIONS {
            let request = append_request("stream", 0, Some(&schema), &batch, compression).unwrap();
            let error = fitting_request(
                "stream",
                0,
                Some(&schema),
                &batch,
                request.encoded_len(),
                compression,
            )
            .unwrap_err();
            assert!(error.contains("single Arrow row"));
            assert!(fitting_request(
                "stream",
                0,
                Some(&schema),
                &batch,
                request.encoded_len() + 1,
                compression,
            )
            .is_ok());
        }
    }

    #[test]
    fn append_requires_success_and_matching_offset() {
        let success = |offset| AppendRowsResponse {
            response: Some(Response::AppendResult(AppendResult { offset })),
            ..Default::default()
        };
        assert!(check_append(success(Some(7)), 7).is_ok());
        assert!(check_append(success(Some(8)), 7)
            .unwrap_err()
            .contains("offset"));
        assert!(check_append(success(None), 7).is_err());
        assert!(check_append(AppendRowsResponse::default(), 0).is_err());
        let mut row_error = success(Some(7));
        row_error.row_errors = vec![RowError::default()];
        assert!(check_append(row_error, 7)
            .unwrap_err()
            .contains("row errors"));
        let error = AppendRowsResponse {
            response: Some(Response::Error(crate::proto::google::rpc::Status {
                code: 3,
                message: "bad row".into(),
                details: vec![],
            })),
            ..Default::default()
        };
        assert!(check_append(error, 0).unwrap_err().contains("bad row"));
    }

    #[test]
    fn commit_checks_embedded_errors_and_commit_time() {
        assert!(check_commit(BatchCommitWriteStreamsResponse::default()).is_err());
        let success = BatchCommitWriteStreamsResponse {
            commit_time: Some(prost_types::Timestamp::default()),
            stream_errors: vec![],
        };
        assert!(check_commit(success.clone()).is_ok());
        assert!(check_commit(BatchCommitWriteStreamsResponse {
            stream_errors: vec![StorageError::default()],
            ..success
        })
        .is_err());
    }

    #[test]
    fn preserves_first_error() {
        let failure = Failure::default();
        failure.record("append failed".into());
        failure.record("finalization failed".into());
        assert_eq!(failure.get().as_deref(), Some("append failed"));
    }

    fn unused_writer() -> PyWriter {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, true)]));
        PyWriter {
            parent: "projects/p/datasets/d/tables/t".into(),
            input_schema: schema.clone(),
            schema: schema.clone(),
            schema_ipc: Arc::new(schema_ipc(&schema).unwrap()),
            worker_count: 2,
            compression: WriteCompression::Uncompressed,
            next_worker: 0,
            workers: Vec::new(),
            failure: Arc::default(),
            abort: Arc::new(AtomicBool::new(false)),
            closed: false,
            runtime: None,
        }
    }

    #[test]
    fn empty_writes_and_idempotent_close_do_not_start_workers() {
        let mut writer = unused_writer();
        writer
            .enqueue(RecordBatch::new_empty(writer.input_schema.clone()))
            .unwrap();
        assert!(writer.runtime.is_none());
        writer.finish(true).unwrap();
        writer.finish(true).unwrap();
        assert!(writer.workers.is_empty());
        assert!(writer.runtime.is_none());
    }

    #[test]
    fn poison_prevents_work_and_commit() {
        let mut writer = unused_writer();
        writer.failure.record("previous failure".into());
        let batch = RecordBatch::try_new(
            writer.schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1]))],
        )
        .unwrap();
        assert_eq!(writer.enqueue(batch).unwrap_err(), "previous failure");
        assert!(writer.runtime.is_none());
        assert_eq!(writer.finish(true).unwrap_err(), "previous failure");
        assert!(writer.closed);
    }

    #[test]
    fn close_joins_all_workers_after_a_failure() {
        let mut writer = unused_writer();
        let runtime = Builder::new_current_thread().enable_all().build().unwrap();
        let completed = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        for index in 0..2 {
            let (sender, mut receiver) = mpsc::channel::<RecordBatch>(1);
            let failure = writer.failure.clone();
            let completed = completed.clone();
            let task = runtime.spawn(async move {
                assert!(receiver.recv().await.is_none());
                if index == 0 {
                    failure.record("append failed".into());
                }
                tokio::task::yield_now().await;
                completed.fetch_add(1, Ordering::SeqCst);
                Some(format!("stream-{index}"))
            });
            writer.workers.push(Worker { sender, task });
        }
        writer.runtime = Some(runtime);
        assert_eq!(writer.finish(true).unwrap_err(), "append failed");
        assert_eq!(completed.load(Ordering::SeqCst), 2);
        assert!(writer.workers.is_empty());
    }

    #[test]
    fn abort_joins_workers_without_committing_their_streams() {
        let mut writer = unused_writer();
        let runtime = Builder::new_current_thread().enable_all().build().unwrap();
        let (sender, mut receiver) = mpsc::channel::<RecordBatch>(1);
        let abort = writer.abort.clone();
        let task = runtime.spawn(async move {
            assert!(receiver.recv().await.is_none());
            assert!(abort.load(Ordering::Acquire));
            Some("pending-stream".into())
        });
        writer.workers.push(Worker { sender, task });
        writer.runtime = Some(runtime);
        writer.finish(false).unwrap();
        assert!(writer.closed);
        writer.finish(true).unwrap();
    }
}

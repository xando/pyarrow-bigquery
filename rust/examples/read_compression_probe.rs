//! Inspect actual gRPC compression on bounded, read-only public-table samples.
//!
//! cargo run --release --example read_compression_probe -- PROJECT /tmp/read-probe.json
//! No tokens, authorization headers, or row contents are logged.

#[allow(dead_code, unused_imports)]
#[path = "../src/proto.rs"]
mod proto;

use std::collections::BTreeSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Instant;

use arrow::buffer::Buffer;
use arrow::ipc::reader::StreamDecoder;
use bytes::Bytes;
use http_body::{Body, Frame};
use serde_json::{json, Value};
use tonic::body::BoxBody;
use tonic::codec::CompressionEncoding;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use tonic::{Request, Status};
use tower::Service;

use proto::bqstorage_v1::{
    big_query_read_client::BigQueryReadClient,
    read_rows_response::Rows,
    read_session::{
        table_read_options::OutputFormatSerializationOptions, Schema, TableReadOptions,
    },
    ArrowSerializationOptions, CreateReadSessionRequest, DataFormat, ReadRowsRequest, ReadSession,
};

#[derive(Default, Debug)]
struct MessageCounter {
    body_bytes: u64,
    messages: u64,
    compressed: u64,
    uncompressed: u64,
    invalid_flags: u64,
    payload_bytes: u64,
    header: [u8; 5],
    header_len: usize,
    remaining: usize,
    current_length: usize,
    current_flag: u8,
}

impl MessageCounter {
    fn completed(&mut self) {
        self.messages += 1;
        self.payload_bytes += self.current_length as u64;
        match self.current_flag {
            0 => self.uncompressed += 1,
            1 => self.compressed += 1,
            _ => self.invalid_flags += 1,
        }
    }

    fn feed(&mut self, mut bytes: &[u8]) {
        self.body_bytes += bytes.len() as u64;
        while !bytes.is_empty() {
            if self.remaining > 0 {
                let take = self.remaining.min(bytes.len());
                self.remaining -= take;
                bytes = &bytes[take..];
                if self.remaining == 0 {
                    self.completed();
                }
                continue;
            }
            let take = (5 - self.header_len).min(bytes.len());
            self.header[self.header_len..self.header_len + take].copy_from_slice(&bytes[..take]);
            self.header_len += take;
            bytes = &bytes[take..];
            if self.header_len == 5 {
                self.current_flag = self.header[0];
                self.current_length =
                    u32::from_be_bytes(self.header[1..5].try_into().unwrap()) as usize;
                self.remaining = self.current_length;
                self.header_len = 0;
                if self.remaining == 0 {
                    self.completed();
                }
            }
        }
    }
}

#[derive(Default)]
struct Observation {
    method: String,
    request_accept_encoding: Option<String>,
    request_encoding: Option<String>,
    response_encoding: Option<String>,
    response_accept_encoding: Option<String>,
    grpc_status: Option<String>,
    counter: MessageCounter,
}

impl Observation {
    fn json(&self) -> Value {
        json!({
            "method": self.method,
            "request_grpc_accept_encoding": self.request_accept_encoding,
            "request_grpc_encoding": self.request_encoding,
            "response_grpc_encoding": self.response_encoding,
            "response_grpc_accept_encoding": self.response_accept_encoding,
            "grpc_status": self.grpc_status,
            "http_body_bytes_observed": self.counter.body_bytes,
            "complete_grpc_messages": self.counter.messages,
            "compressed_messages": self.counter.compressed,
            "uncompressed_messages": self.counter.uncompressed,
            "invalid_compression_flags": self.counter.invalid_flags,
            "complete_message_payload_bytes": self.counter.payload_bytes,
            "partial_header_bytes": self.counter.header_len,
            "remaining_message_bytes": self.counter.remaining,
        })
    }
}

type Observations = Arc<Mutex<Vec<Arc<Mutex<Observation>>>>>;

struct ObservedBody {
    inner: BoxBody,
    observation: Arc<Mutex<Observation>>,
}

impl Body for ObservedBody {
    type Data = Bytes;
    type Error = Status;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let result = Pin::new(&mut self.inner).poll_frame(cx);
        if let Poll::Ready(Some(Ok(frame))) = &result {
            let mut observation = self.observation.lock().unwrap();
            if let Some(data) = frame.data_ref() {
                observation.counter.feed(data);
            }
            if let Some(trailers) = frame.trailers_ref() {
                observation.grpc_status = header(trailers, "grpc-status");
            }
        }
        result
    }
}

fn header(headers: &http::HeaderMap, key: &str) -> Option<String> {
    headers
        .get(key)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
}

#[derive(Clone)]
struct ObservedChannel {
    inner: Channel,
    observations: Observations,
}

impl Service<http::Request<BoxBody>> for ObservedChannel {
    type Response = http::Response<BoxBody>;
    type Error = tonic::transport::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: http::Request<BoxBody>) -> Self::Future {
        let observation = Arc::new(Mutex::new(Observation {
            method: request.uri().path().to_owned(),
            request_accept_encoding: header(request.headers(), "grpc-accept-encoding"),
            request_encoding: header(request.headers(), "grpc-encoding"),
            ..Default::default()
        }));
        let observations = self.observations.clone();
        let future = self.inner.call(request);
        Box::pin(async move {
            let response = future.await?;
            let (parts, body) = response.into_parts();
            {
                let mut entry = observation.lock().unwrap();
                entry.response_encoding = header(&parts.headers, "grpc-encoding");
                entry.response_accept_encoding = header(&parts.headers, "grpc-accept-encoding");
                entry.grpc_status = header(&parts.headers, "grpc-status");
            }
            observations.lock().unwrap().push(observation.clone());
            let observed = ObservedBody {
                inner: body,
                observation,
            };
            Ok(http::Response::from_parts(
                parts,
                tonic::body::boxed(observed),
            ))
        })
    }
}

fn authorized<T>(
    body: T,
    token: &str,
    routing: &str,
) -> Result<Request<T>, Box<dyn std::error::Error>> {
    let mut request = Request::new(body);
    request
        .metadata_mut()
        .insert("authorization", format!("Bearer {token}").parse()?);
    request
        .metadata_mut()
        .insert("x-goog-request-params", routing.parse()?);
    request.set_timeout(std::time::Duration::from_secs(60));
    Ok(request)
}

struct Sample {
    table: &'static str,
    columns: &'static [&'static str],
    restriction: &'static str,
}

fn arrow_buffer_codec(bytes: &[u8]) -> Result<Option<String>, Box<dyn std::error::Error>> {
    let first = bytes.get(..4).ok_or("Short Arrow IPC prefix")?;
    let (length, start) = if first == [0xff; 4] {
        (bytes.get(4..8).ok_or("Short Arrow IPC continuation")?, 8)
    } else {
        (first, 4)
    };
    let length = u32::from_le_bytes(length.try_into()?) as usize;
    let metadata = bytes
        .get(start..start + length)
        .ok_or("Short Arrow IPC metadata")?;
    let message = arrow::ipc::root_as_message(metadata)
        .map_err(|error| format!("Invalid Arrow IPC metadata: {error:?}"))?;
    let batch = message
        .header_as_record_batch()
        .ok_or("Expected Arrow RecordBatch metadata")?;
    Ok(batch
        .compression()
        .map(|compression| format!("{:?}", compression.codec())))
}

async fn probe(
    channel: Channel,
    token: &str,
    project: &str,
    sample: &Sample,
    mode: &str,
    observations: Observations,
) -> Result<Value, Box<dyn std::error::Error>> {
    let service = ObservedChannel {
        inner: channel,
        observations: observations.clone(),
    };
    let mut client = BigQueryReadClient::new(service).max_decoding_message_size(256 * 1024 * 1024);
    if mode != "identity" {
        client = client.accept_compressed(CompressionEncoding::Gzip);
    }
    let parts: Vec<_> = sample.table.split('.').collect();
    let table = format!(
        "projects/{}/datasets/{}/tables/{}",
        parts[0], parts[1], parts[2]
    );
    let parent = format!("projects/{project}");
    let session = client
        .create_read_session(authorized(
            CreateReadSessionRequest {
                parent: parent.clone(),
                read_session: Some(ReadSession {
                    table,
                    data_format: DataFormat::Arrow as i32,
                    read_options: Some(TableReadOptions {
                        selected_fields: sample
                            .columns
                            .iter()
                            .map(|column| (*column).into())
                            .collect(),
                        row_restriction: sample.restriction.into(),
                        output_format_serialization_options: Some(
                            OutputFormatSerializationOptions::ArrowSerializationOptions(
                                ArrowSerializationOptions {
                                    buffer_compression: 0,
                                    ..Default::default()
                                },
                            ),
                        ),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                max_stream_count: 1,
                preferred_min_stream_count: 0,
            },
            token,
            &format!("parent={parent}"),
        )?)
        .await?
        .into_inner();
    let Some(Schema::ArrowSchema(schema)) = session.schema else {
        return Err("Read session did not return an Arrow schema".into());
    };
    let mut decoder = StreamDecoder::new();
    decoder.decode(&mut Buffer::from(schema.serialized_schema))?;
    // Keep session creation plain: this variant only compresses ReadRows.
    if mode == "send_and_accept_gzip" {
        client = client.send_compressed(CompressionEncoding::Gzip);
    }
    let mut decoded_rows = 0_u64;
    let mut decoded_batches = 0_u64;
    let mut arrow_bytes = 0_u64;
    let mut serialized_arrow_bytes = 0_u64;
    let mut responses = 0_u64;
    let mut truncated = false;
    let mut arrow_codecs = BTreeSet::new();
    let start = Instant::now();
    for stream in session.streams {
        let request = ReadRowsRequest {
            read_stream: stream.name.clone(),
            offset: 0,
        };
        let mut messages = client
            .read_rows(authorized(
                request,
                token,
                &format!("read_stream={}", stream.name),
            )?)
            .await?
            .into_inner();
        while let Some(response) = messages.message().await? {
            responses += 1;
            let expected_rows = response.row_count;
            let mut response_rows = 0_i64;
            if let Some(Rows::ArrowRecordBatch(batch)) = response.rows {
                serialized_arrow_bytes += batch.serialized_record_batch.len() as u64;
                arrow_codecs.insert(
                    arrow_buffer_codec(&batch.serialized_record_batch)?
                        .unwrap_or_else(|| "uncompressed".into()),
                );
                let mut buffer = Buffer::from(batch.serialized_record_batch);
                while !buffer.is_empty() {
                    let Some(batch) = decoder.decode(&mut buffer)? else {
                        break;
                    };
                    decoded_rows += batch.num_rows() as u64;
                    response_rows += batch.num_rows() as i64;
                    decoded_batches += 1;
                    arrow_bytes += batch.get_array_memory_size() as u64;
                }
            }
            if response_rows != expected_rows {
                return Err(format!(
                    "Decoded {response_rows} rows, response reported {expected_rows}"
                )
                .into());
            }
            // This is a capability probe, not an unbounded public-table scan.
            if responses >= 64 || serialized_arrow_bytes >= 32 * 1024 * 1024 {
                truncated = true;
                break;
            }
        }
        if truncated {
            break;
        }
    }
    let rpc_observations: Vec<_> = observations
        .lock()
        .unwrap()
        .iter()
        .map(|entry| entry.lock().unwrap().json())
        .collect();
    Ok(json!({
        "table": sample.table, "columns": sample.columns, "row_restriction": sample.restriction,
        "mode": mode, "arrow_buffer_compression": "disabled", "decoded_rows": decoded_rows,
        "arrow_buffer_codecs_observed": arrow_codecs,
        "decoded_batches": decoded_batches, "decoded_arrow_array_bytes": arrow_bytes,
        "serialized_arrow_bytes": serialized_arrow_bytes,
        "read_rows_responses": responses, "sample_truncated": truncated,
        "read_seconds": start.elapsed().as_secs_f64(), "rpcs": rpc_observations,
    }))
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().collect();
    if args.len() != 3 {
        return Err("Usage: read_compression_probe PROJECT OUTPUT.json".into());
    }
    let samples = [
        Sample {
            table: "bigquery-public-data.samples.shakespeare",
            columns: &[],
            restriction: "",
        },
        Sample {
            table: "bigquery-public-data.austin_crime.crime",
            columns: &["unique_key", "description", "primary_type", "address"],
            restriction: "",
        },
        Sample {
            table: "bigquery-public-data.usa_names.usa_1910_current",
            columns: &["state", "gender", "year", "name", "number"],
            restriction: "year = 2020",
        },
    ];
    let provider = gcp_auth::provider().await?;
    let token = provider
        .token(&["https://www.googleapis.com/auth/bigquery.readonly"])
        .await?;
    let channel = Endpoint::from_static("https://bigquerystorage.googleapis.com")
        .connect_timeout(std::time::Duration::from_secs(20))
        .tls_config(ClientTlsConfig::new().with_native_roots())?
        .connect()
        .await?;
    let mut results = Vec::new();
    for sample in &samples {
        for mode in ["identity", "accept_gzip", "send_and_accept_gzip"] {
            eprintln!("Probing {}: {mode}", sample.table);
            let observations = Observations::default();
            match probe(
                channel.clone(),
                token.as_str(),
                &args[1],
                sample,
                mode,
                observations.clone(),
            )
            .await
            {
                Ok(result) => {
                    eprintln!(
                        "  {} decoded rows; {} responses; truncated={}",
                        result["decoded_rows"],
                        result["read_rows_responses"],
                        result["sample_truncated"]
                    );
                    for rpc in result["rpcs"].as_array().unwrap() {
                        if rpc["method"].as_str().unwrap().ends_with("/ReadRows") {
                            eprintln!("  offered={}; response encoding={}; compressed messages={}; plain messages={}",
                                      rpc["request_grpc_accept_encoding"], rpc["response_grpc_encoding"],
                                      rpc["compressed_messages"], rpc["uncompressed_messages"]);
                        }
                    }
                    results.push(result);
                }
                Err(error) => {
                    eprintln!("  ERROR: {error}");
                    let rpcs: Vec<_> = observations
                        .lock()
                        .unwrap()
                        .iter()
                        .map(|entry| entry.lock().unwrap().json())
                        .collect();
                    results.push(
                        json!({"table": sample.table, "mode": mode, "error": error.to_string(), "rpcs": rpcs}),
                    );
                }
            }
            std::fs::write(
                &args[2],
                serde_json::to_string_pretty(&json!({
                    "project": args[1], "scope": "read-only public-table compression capability probe",
                    "body_byte_counts_exclude_http2_and_tls_overhead": true, "results": results,
                }))? + "\n",
            )?;
        }
    }
    eprintln!("Saved {}", args[2]);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts_compressed_flags_across_every_chunk_boundary() {
        let wire = [
            0, 0, 0, 0, 3, b'a', b'b', b'c', 1, 0, 0, 0, 2, b'x', b'y', 0, 0, 0, 0, 0,
        ];
        for chunk_size in 1..=wire.len() {
            let mut counter = MessageCounter::default();
            for bytes in wire.chunks(chunk_size) {
                counter.feed(bytes);
            }
            assert_eq!(counter.messages, 3);
            assert_eq!(counter.compressed, 1);
            assert_eq!(counter.uncompressed, 2);
            assert_eq!(counter.body_bytes, wire.len() as u64);
            assert_eq!(counter.payload_bytes, 5);
            assert_eq!(counter.header_len, 0);
            assert_eq!(counter.remaining, 0);
        }
    }

    #[test]
    fn incomplete_payload_is_not_counted_as_a_complete_message() {
        let mut counter = MessageCounter::default();
        counter.feed(&[1, 0, 0]);
        assert_eq!(counter.messages, 0);
        counter.feed(&[0, 3, b'a']);
        assert_eq!(counter.remaining, 2);
        assert_eq!(counter.messages, 0);
        counter.feed(b"bc");
        assert_eq!(counter.messages, 1);
        assert_eq!(counter.compressed, 1);
    }

    #[test]
    fn observes_actual_arrow_buffer_compression() {
        use arrow::array::Int64Array;
        use arrow::ipc::writer::{
            write_message, DictionaryTracker, IpcDataGenerator, IpcWriteOptions,
        };
        use arrow::record_batch::RecordBatch;
        let batch = RecordBatch::try_from_iter([(
            "value",
            Arc::new(Int64Array::from(vec![7; 100])) as arrow::array::ArrayRef,
        )])
        .unwrap();
        for (codec, expected) in [
            (None, None),
            (
                Some(arrow::ipc::CompressionType::LZ4_FRAME),
                Some("LZ4_FRAME"),
            ),
            (Some(arrow::ipc::CompressionType::ZSTD), Some("ZSTD")),
        ] {
            let options = IpcWriteOptions::default()
                .try_with_compression(codec)
                .unwrap();
            let (_, encoded) = IpcDataGenerator::default()
                .encoded_batch(&batch, &mut DictionaryTracker::new(false), &options)
                .unwrap();
            let mut bytes = Vec::new();
            write_message(&mut bytes, encoded, &options).unwrap();
            assert_eq!(arrow_buffer_codec(&bytes).unwrap().as_deref(), expected);
        }
    }
}

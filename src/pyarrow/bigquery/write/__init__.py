from __future__ import annotations

import collections
import datetime
import logging
import multiprocessing
import os
import queue
import shutil
import tempfile
import threading
import time
import traceback
import warnings
from concurrent.futures import CancelledError
from typing import Literal

import grpc
import pyarrow as pa
import pyarrow.feather as fa
from google.api_core import retry
from google.api_core.exceptions import NotFound, Unknown
from google.cloud import bigquery, bigquery_storage_v1
from google.cloud.bigquery_storage_v1.services.big_query_write.transports.grpc import (
    BigQueryWriteGrpcTransport,
)
from google.cloud.bigquery_storage_v1.writer import AppendRowsStream
from google.protobuf import descriptor_pb2

from .. import _rust, some_itertools
from . import pa_to_bq, pa_to_pb, upload

logger = logging.getLogger(__name__)

_ENGINES = ("python", "rust")
_COMPRESSIONS = {
    "python": (None, "gzip"),
    "rust": (None, "gzip", "lz4", "zstd"),
}

_QUEUE_DONE = "done"
_QUEUE_ERROR = "error"
_QUEUE_GET_TIMEOUT = 0.25
_WORKER_JOIN_TIMEOUT = 5.0

Stream = collections.namedtuple("Stream", ["append_rows_stream", "write_stream"])


def _queue_error(exc: BaseException):
    return (_QUEUE_ERROR, (type(exc).__name__, str(exc), traceback.format_exc()))


def _close_client_transport(client):
    if not client:
        return
    transport = getattr(client, "transport", None)
    close = getattr(transport, "close", None)
    if close:
        try:
            close()
        except Exception:
            logger.exception("Failed to close BigQuery Storage client transport")


def _bq_create_table(*, project, location, schema, expire, overwrite):
    bq_schema = pa_to_bq.generate(schema)
    client = bigquery.Client(project=project)

    if overwrite:
        client.delete_table(location, not_found_ok=True)

    table = bigquery.Table(location, schema=bq_schema)

    client.create_table(table)

    if expire:
        table.expires = datetime.datetime.now() + datetime.timedelta(seconds=expire)
        client.update_table(table, ["expires"])

    logger.debug(f"Created BigQuery table '{location}'")


def _bq_write_create_stream(
    write_client: bigquery_storage_v1.BigQueryWriteClient, parent, protobuf_definition
):
    write_stream = write_client.create_write_stream(
        parent=parent,
        write_stream=bigquery_storage_v1.types.WriteStream(
            type=bigquery_storage_v1.types.WriteStream.Type.PENDING
        ),
        retry=retry.Retry(predicate=retry.if_exception_type(Unknown, NotFound)),
    )

    proto_schema = bigquery_storage_v1.types.ProtoSchema()
    proto_descriptor = descriptor_pb2.DescriptorProto()
    protobuf_definition.CopyToProto(proto_descriptor)
    proto_schema.proto_descriptor = proto_descriptor

    proto_data = bigquery_storage_v1.types.AppendRowsRequest.ProtoData()
    proto_data.writer_schema = proto_schema

    request_template = bigquery_storage_v1.types.AppendRowsRequest()
    request_template.write_stream = write_stream.name
    request_template.proto_rows = proto_data

    append_rows_stream = AppendRowsStream(write_client, request_template)

    return Stream(
        write_stream=write_stream,
        append_rows_stream=append_rows_stream,
    )


def _bq_storage_close_stream(write_client, stream, expected_rows):
    # All append futures must have succeeded before closing the connection.
    stream.append_rows_stream.close()
    result = write_client.finalize_write_stream(name=stream.write_stream.name)
    if result.row_count != expected_rows:
        raise RuntimeError(
            f"FinalizeWriteStream returned {result.row_count} rows, expected {expected_rows}"
        )
    logger.debug(f"Stream '{stream.write_stream.name}' finalized")
    return stream.write_stream.name


def _bq_storage_commit_streams(parent, stream_names):
    if not stream_names:
        return
    write_client = bigquery_storage_v1.BigQueryWriteClient()
    try:
        result = write_client.batch_commit_write_streams(
            bigquery_storage_v1.types.BatchCommitWriteStreamsRequest(
                parent=parent, write_streams=stream_names,
            )
        )
        if result.stream_errors:
            raise RuntimeError(f"BatchCommitWriteStreams failed: {result.stream_errors}")
        if not bigquery_storage_v1.types.BatchCommitWriteStreamsResponse.pb(result).HasField(
            "commit_time"
        ):
            raise RuntimeError("BatchCommitWriteStreams returned no commit time")
    finally:
        _close_client_transport(write_client)


def _stream_worker(
    parent: str,
    schema_protobuf,
    queue_results,
    queue_worker_status,
    compression: str | None = None,
    stop_event=None,
):
    write_client = None
    stream = None
    try:
        if compression == "gzip":
            # Pass a channel instance for compatibility with Storage client 2.18.
            channel = BigQueryWriteGrpcTransport.create_channel(
                compression=grpc.Compression.Gzip,
                options=[
                    ("grpc.max_send_message_length", -1),
                    ("grpc.max_receive_message_length", -1),
                ],
            )
            transport = BigQueryWriteGrpcTransport(channel=channel)
            write_client = bigquery_storage_v1.BigQueryWriteClient(transport=transport)
        else:
            write_client = bigquery_storage_v1.BigQueryWriteClient()
        stream = _bq_write_create_stream(write_client, parent, schema_protobuf)

        offset = 0

        while True:
            if stop_event is not None and stop_event.is_set():
                return
            try:
                element = queue_results.get(timeout=_QUEUE_GET_TIMEOUT)
            except queue.Empty:
                continue
            if element is None:
                break
            if stop_event is not None and stop_event.is_set():
                return

            table = fa.read_table(element)

            if stop_event is None:
                upload.upload_data(stream, table, schema_protobuf, offset)
            else:
                upload.upload_data(stream, table, schema_protobuf, offset, stop_event=stop_event)

            os.remove(element)

            offset += table.num_rows

        if stop_event is not None and stop_event.is_set():
            return
        stream_name = _bq_storage_close_stream(write_client, stream, offset)
        stream = None
        queue_worker_status.put((_QUEUE_DONE, stream_name))
    except CancelledError as exc:
        if stop_event is None or not stop_event.is_set():
            queue_worker_status.put(_queue_error(exc))
    except BaseException as exc:
        queue_worker_status.put(_queue_error(exc))
    finally:
        if stream is not None:
            try:
                # On failure, close the RPC but never finalize/commit partial work.
                stream.append_rows_stream.close()
            except Exception:
                logger.exception("Failed to close BigQuery write stream")
        _close_client_transport(write_client)


class writer:
    """Stream Arrow data to BigQuery using Python or Rust workers.

    ``engine="python"`` preserves the Python worker pool. ``engine="rust"``
    passes batches to the bundled extension over the Arrow C Data Interface;
    ``worker_count`` controls native streams and ``worker_type`` is ignored.
    Both engines commit their pending streams together only after every append
    and finalization succeeds and the context exits without an exception.
    ``compression="gzip"`` compresses gRPC requests with either engine.
    Rust also supports ``"lz4"`` and ``"zstd"`` for Arrow buffer compression.
    """

    def __init__(
        self,
        schema: pa.Schema,
        where: str,
        *,
        project: str | None = None,
        table_create: bool = True,
        table_expire: int | None = None,
        table_overwrite: bool = False,
        worker_count: int = multiprocessing.cpu_count(),
        worker_type: type[threading.Thread] | type[multiprocessing.Process] = threading.Thread,
        engine: Literal["python", "rust"] = "python",
        compression: Literal["gzip", "lz4", "zstd"] | None = None,
    ):
        if engine not in _ENGINES:
            raise ValueError(
                f"Unsupported engine {engine!r}, expected one of {_ENGINES}"
            )
        if compression not in _COMPRESSIONS[engine]:
            raise ValueError(
                f"Unsupported write compression {compression!r} for engine={engine!r}, "
                f"expected one of {_COMPRESSIONS[engine]}"
            )
        if engine == "rust" and (
            not isinstance(worker_count, int) or worker_count < 1
        ):
            raise ValueError("worker_count must be a positive integer for engine=\"rust\"")

        self.project = project
        self.where = where
        self.schema = schema
        self.engine = engine
        self.compression = compression
        self._rust_writer = None
        self._python_open = False

        self.table_create = table_create
        self.table_expire = table_expire
        self.table_overwrite = table_overwrite

        self.worker_count = worker_count
        self.worker_type = worker_type
        if self.worker_type not in (threading.Thread, multiprocessing.Process):
            raise ValueError(
                f"Unsupported worker type {worker_type}, "
                "expected threading.Thread or multiprocessing.Process"
            )
        if engine == "rust" and worker_type is not threading.Thread:
            warnings.warn(
                '`worker_type` is ignored when `engine="rust"`: '
                "the Rust path manages concurrency internally.",
                stacklevel=2,
            )

        project_id, dataset_id, table_id = where.split(".")

        self.parent = f"projects/{project_id}/datasets/{dataset_id}/tables/{table_id}"

        if not self.project:
            self.project = project_id

    def __enter__(self):
        self.t0 = time.time()
        if self.engine == "rust":
            return self._enter_rust()

        if self._python_open:
            raise RuntimeError("BigQuery writer is already open")
        self.workers = []
        self._write_streams = []
        self._workers_done = 0
        self._failure = None
        self._closing = False
        queue_maxsize = max(4, self.worker_count * 2)
        if self.worker_type == threading.Thread:
            self.queue_results = queue.Queue(maxsize=queue_maxsize)
            self.queue_worker_status = queue.Queue()
            self._stop_event = threading.Event()
        else:
            self.queue_results = multiprocessing.Queue(maxsize=queue_maxsize)
            self.queue_worker_status = multiprocessing.Queue()
            self._stop_event = multiprocessing.Event()
        self.temp_dir = tempfile.mkdtemp()
        try:
            self.schema_protobuf = pa_to_pb.generate(self.schema)
            if self.table_create:
                _bq_create_table(
                    project=self.project,
                    location=self.where,
                    schema=self.schema,
                    expire=self.table_expire,
                    overwrite=self.table_overwrite,
                )

            for _ in range(self.worker_count):
                worker = self.worker_type(
                    target=_stream_worker,
                    args=(
                        self.parent,
                        self.schema_protobuf,
                        self.queue_results,
                        self.queue_worker_status,
                        self.compression,
                        self._stop_event,
                    ),
                )
                if self.worker_type == threading.Thread:
                    worker.daemon = True
                worker.start()
                self.workers.append(worker)
        except BaseException:
            self._cleanup_python()
            raise

        self._python_open = True
        return self

    def _enter_rust(self):
        if self._rust_writer is not None:
            raise RuntimeError("BigQuery writer is already open")
        # The native constructor validates the schema without opening streams.
        # Do this before table_overwrite can delete the destination table.
        rust_writer = _rust.PyWriter(
            parent=self.parent,
            schema=self.schema,
            worker_count=self.worker_count,
            compression=self.compression,
        )
        try:
            if self.table_create:
                _bq_create_table(
                    project=self.project,
                    location=self.where,
                    schema=self.schema,
                    expire=self.table_expire,
                    overwrite=self.table_overwrite,
                )
        except BaseException:
            try:
                rust_writer.close(commit=False)
            except Exception:
                logger.exception("Failed to close Rust BigQuery writer after setup failure")
            raise
        self._rust_writer = rust_writer
        return self

    def _open_rust_writer(self):
        if self._rust_writer is None:
            raise RuntimeError("BigQuery writer is not open; use it inside a with block")
        return self._rust_writer

    def write_table(self, table):
        if self.engine == "python":
            self._check_python_write()
        if self.engine == "rust":
            rust_writer = self._open_rust_writer()
            for batch in table.to_batches():
                rust_writer.write_batch(batch)
            return

        fd, element = tempfile.mkstemp(dir=self.temp_dir)
        os.close(fd)
        try:
            fa.write_feather(table, element)
            self._put_work(element)
        except BaseException as exc:
            self._record_failure(exc)
            os.remove(element)
            raise

    def write_batch(self, batch):
        if self.engine == "python":
            self._check_python_write()
        if self.engine == "rust":
            self._open_rust_writer().write_batch(batch)
            return

        fd, element = tempfile.mkstemp(dir=self.temp_dir)
        os.close(fd)
        try:
            fa.write_feather(pa.Table.from_batches([batch]), element)
            self._put_work(element)
        except BaseException as exc:
            self._record_failure(exc)
            os.remove(element)
            raise

    def _process_worker_status(self, kind: str, payload, workers_done: int):
        if kind == _QUEUE_DONE:
            if payload is not None:
                self._write_streams.append(payload)
            return workers_done + 1

        if kind == _QUEUE_ERROR:
            error_type, error_message, formatted_traceback = payload
            raise RuntimeError(
                f"BigQuery write worker failed with {error_type}: {error_message}\n"
                f"{formatted_traceback}"
            )

        raise RuntimeError(f"Unexpected BigQuery write worker message: {(kind, payload)!r}")

    def _record_failure(self, error):
        if self._failure is None:
            self._failure = error
        self._stop_event.set()

    def _collect_worker_status(self, wait=False):
        while True:
            try:
                message = self.queue_worker_status.get(timeout=_QUEUE_GET_TIMEOUT if wait else 0)
            except queue.Empty:
                return
            wait = False
            self._workers_done = self._process_worker_status(*message, self._workers_done)
            if self._workers_done > self.worker_count:
                raise RuntimeError("BigQuery write workers sent duplicate completion messages")

    def _check_worker_status(self, wait=False):
        if self._failure is not None:
            raise self._failure
        try:
            self._collect_worker_status(wait=wait)
            for idx, worker in enumerate(self.workers):
                exitcode = getattr(worker, "exitcode", None)
                if exitcode not in (None, 0):
                    raise RuntimeError(f"BigQuery write worker {idx} exited with code {exitcode}")
            if self.workers and all(not worker.is_alive() for worker in self.workers):
                # A worker can publish its last status between our first poll
                # and the liveness check. Recheck before declaring it missing.
                self._collect_worker_status()
                if self._workers_done < self.worker_count:
                    raise RuntimeError("BigQuery write workers exited before signaling completion")
            if not self._closing and self._workers_done:
                raise RuntimeError("BigQuery write workers stopped before input was closed")
        except BaseException as exc:
            self._record_failure(exc)
            raise

    def _check_python_write(self):
        if not self._python_open or self._closing:
            raise RuntimeError("BigQuery writer is not open; use it inside a with block")
        self._check_worker_status()

    def _put_work(self, element):
        while True:
            self._check_worker_status()
            if element is None and self._workers_done == self.worker_count:
                return
            try:
                self.queue_results.put(element, timeout=_QUEUE_GET_TIMEOUT)
                return
            except queue.Full:
                continue

    def _drain_worker_status(self):
        while self._workers_done < self.worker_count:
            self._check_worker_status(wait=True)

    def _join_python_workers(self, abort=False):
        if not abort:
            # Completion has already been acknowledged by every worker. Allow
            # client and process teardown to finish without killing healthy work.
            for worker in self.workers:
                worker.join()
            return
        deadline = time.monotonic() + _WORKER_JOIN_TIMEOUT
        for worker in self.workers:
            worker.join(timeout=max(0, deadline - time.monotonic()))
        alive_workers = [worker for worker in self.workers if worker.is_alive()]
        for worker in alive_workers:
            terminate = getattr(worker, "terminate", None)
            if terminate:
                terminate()
        for worker in alive_workers:
            if getattr(worker, "terminate", None):
                worker.join(timeout=_WORKER_JOIN_TIMEOUT)
        still_alive = [worker for worker in self.workers if worker.is_alive()]
        if still_alive:
            raise RuntimeError(f"{len(still_alive)} BigQuery write worker(s) did not stop cleanly")

    def _cleanup_python(self):
        self._stop_event.set()
        try:
            self._join_python_workers(abort=True)
        except Exception as exc:
            self._record_failure(exc)
            logger.exception("Failed to stop Python BigQuery workers during cleanup")
        finally:
            self._python_open = False
            if self.worker_type == multiprocessing.Process:
                for worker_queue in (self.queue_results, self.queue_worker_status):
                    # Aborted workers may leave queued paths unread. Do not
                    # wait forever for a producer feeder to flush to no reader.
                    worker_queue.cancel_join_thread()
                    worker_queue.close()
            shutil.rmtree(self.temp_dir, ignore_errors=True)

    def __exit__(self, exc_type, *_):
        if self.engine == "rust":
            self._exit_rust(exc_type)
            logger.debug(f"Time taken: {time.time() - self.t0:.2f}")
            return

        self._closing = True
        try:
            if exc_type is None and self._failure is None:
                for _ in range(self.worker_count):
                    self._put_work(None)
                # Wait for real upload/finalization completion before applying
                # the shutdown timeout. Slow healthy uploads are not killed.
                self._drain_worker_status()
                self._join_python_workers()
                self._check_worker_status()
                _bq_storage_commit_streams(self.parent, self._write_streams)
        except BaseException as exc:
            self._record_failure(exc)
        finally:
            self._cleanup_python()

        if self._failure is not None:
            if exc_type is None:
                raise self._failure
            logger.error("Python BigQuery writer aborted: %s", self._failure)
        logger.debug(f"Time taken: {time.time() - self.t0:.2f}")

    def _exit_rust(self, exc_type):
        rust_writer = self._rust_writer
        self._rust_writer = None
        if rust_writer is None:
            return
        try:
            rust_writer.close(commit=exc_type is None)
        except Exception:
            if exc_type is None:
                raise
            logger.exception("Failed to close Rust BigQuery writer during exception cleanup")


def write_table(
    table: pa.Table,
    where: str,
    *,
    project: str | None = None,
    table_create: bool = True,
    table_expire: int | None = None,
    table_overwrite: bool = False,
    worker_count: int = multiprocessing.cpu_count(),
    worker_type: type[threading.Thread] | type[multiprocessing.Process] = threading.Thread,
    batch_size: int = 10,
    engine: Literal["python", "rust"] = "python",
    compression: Literal["gzip", "lz4", "zstd"] | None = None,
):
    assert table.num_rows > 0, "Table is empty"

    with writer(
        where=where,
        schema=table.schema,
        project=project,
        table_create=table_create,
        table_expire=table_expire,
        table_overwrite=table_overwrite,
        worker_count=worker_count,
        worker_type=worker_type,
        engine=engine,
        compression=compression,
    ) as w:
        for table_chunk in some_itertools.to_chunks(table, batch_size):
            w.write_table(table_chunk)

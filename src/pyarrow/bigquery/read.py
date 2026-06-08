from __future__ import annotations

import datetime
import inspect
import logging
import multiprocessing
import queue
import threading
import time
import traceback
import warnings
from typing import Literal

from google.cloud import bigquery, bigquery_storage
from google.cloud.exceptions import NotFound

import pyarrow as pa

from . import _rust, exchange, some_itertools

PYARROW_COMPRESSIONS = {
    None: bigquery_storage.ArrowSerializationOptions.CompressionCodec.COMPRESSION_UNSPECIFIED,
    "lz4": bigquery_storage.ArrowSerializationOptions.CompressionCodec.LZ4_FRAME,
    "zstd": bigquery_storage.ArrowSerializationOptions.CompressionCodec.ZSTD,
}

# Recognized values for the `compression` parameter. The Rust engine passes
# the string straight through to the Rust extension; the Python engine maps
# via PYARROW_COMPRESSIONS above.
_RUST_COMPRESSION_CHOICES = (None, "lz4", "zstd")

_ENGINES = ("python", "rust")

logger = logging.getLogger(__name__)

_QUEUE_RESULT = "result"
_QUEUE_DONE = "done"
_QUEUE_ERROR = "error"
_QUEUE_GET_TIMEOUT = 0.25
_WORKER_JOIN_TIMEOUT = 5.0


def _queue_result(key: str):
    return (_QUEUE_RESULT, key)


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


def _bq_table_exists(project: str, location: str):
    client = bigquery.Client(project=project)
    try:
        client.get_table(location)
        logger.debug(f"Table {location} indeed exists")
    except NotFound as e:
        logger.debug(f"Table {location} is not found")
        raise e
    finally:
        _close_client_transport(client)


def _bq_delete_table(project: str, location: str):
    client = bigquery.Client(project=project)
    try:
        client.delete_table(location, not_found_ok=True)
        logger.debug(f"Deleted table {location}")
    except Exception:
        logger.exception(f"Failed to delete table {location}")
    finally:
        _close_client_transport(client)


def _bq_read_create_streams(
    read_client: bigquery_storage.BigQueryReadClient,
    parent: str,
    source: str,
    selected_fields: list | None,
    row_restrictions: str | None,
    max_stream_count: int,
    compression: bigquery_storage.ArrowSerializationOptions.CompressionCodec,
) -> tuple[list[str], pa.Schema]:
    project, dataset, table = source.split(".")
    read_session = bigquery_storage.ReadSession(
        table=f"projects/{project}/datasets/{dataset}/tables/{table}",
        data_format=bigquery_storage.DataFormat.ARROW,
        read_options={
            "selected_fields": selected_fields,
            "row_restriction": row_restrictions,
            "arrow_serialization_options": bigquery_storage.ArrowSerializationOptions(
                buffer_compression=compression
            ),
        },
    )

    read_session = read_client.create_read_session(
        parent=f"projects/{parent}",
        read_session=read_session,
        max_stream_count=max_stream_count,
    )

    schema_buffer = pa.py_buffer(read_session.arrow_schema.serialized_schema)
    schema = pa.ipc.read_schema(schema_buffer)

    return ([s.name for s in read_session.streams], schema)


def _stream_worker(
    read_streams: list[str],
    table_schema: pa.Schema,
    batch_size: int,
    queue_results: multiprocessing.Queue,
    ipc_exchange,
):
    read_client = None
    try:
        read_client = bigquery_storage.BigQueryReadClient()
        batches = []
        for stream in read_streams:
            t = time.time()

            for message in read_client.read_rows(stream):
                record_batch = pa.ipc.read_record_batch(
                    message.arrow_record_batch.serialized_record_batch,
                    table_schema,
                )

                batches.append(record_batch)

                while batches and sum(b.num_rows for b in batches) >= batch_size:
                    table = pa.Table.from_batches(batches)
                    queue_results.put(_queue_result(ipc_exchange.store(table[:batch_size])))
                    batches = table[batch_size:].to_batches()

            logger.debug(f"Stream {stream} done in {time.time() - t:.2f} seconds")

        if batches:
            queue_key = ipc_exchange.store(pa.Table.from_batches(batches))
            queue_results.put(_queue_result(queue_key))

        queue_results.put((_QUEUE_DONE, None))
    except BaseException as exc:
        queue_results.put(_queue_error(exc))
    finally:
        _close_client_transport(read_client)


class reader:
    """Context manager that streams a BigQuery table as `pa.Table` chunks.

    Two engines are available, selected via `engine=`:

    - ``engine="python"`` (default): the original implementation. Spawns a
      pool of threads or processes (per ``worker_type``), each consuming one
      or more BigQuery Storage Read streams; results travel through the
      configured ``ipc_exchange``. Behaviour is identical to 0.6.7.
    - ``engine="rust"``: uses the bundled Rust extension
      (``pyarrow.bigquery._rust.PyReader``) to manage gRPC, Arrow IPC
      decode and stream-level concurrency. ``worker_type`` and
      ``ipc_exchange`` are accepted for signature compatibility but are
      ignored — a warning is emitted if you pass non-default values.
    """

    def __init__(
        self,
        source: str,
        *,
        project: str | None = None,
        columns: list | None = None,
        row_restrictions: str | None = None,
        worker_count: int = multiprocessing.cpu_count(),
        worker_type: type[threading.Thread] | type[multiprocessing.Process] = threading.Thread,
        ipc_exchange: exchange.ConcurrencyCompatible | None = None,
        batch_size: int = 100,
        compression: str | None = None,
        delete_source_on_exit: bool = False,
        engine: Literal["python", "rust"] = "python",
    ):
        if engine not in _ENGINES:
            raise ValueError(
                f"Unsupported engine {engine!r}, expected one of {_ENGINES}"
            )

        if worker_type not in (threading.Thread, multiprocessing.Process):
            raise ValueError(
                f"Unsupported worker type {worker_type}, "
                f"expected threading.Thread or multiprocessing.Process"
            )

        self.source = source
        self.delete_source_on_exit = delete_source_on_exit
        self.columns = columns
        self.row_restrictions = row_restrictions
        self.worker_count = worker_count
        self.worker_type = worker_type
        self.batch_size = batch_size
        self.engine = engine

        if engine == "python":
            assert compression in PYARROW_COMPRESSIONS, (
                f"Compression {compression} not supported, "
                f"available: {list(PYARROW_COMPRESSIONS.keys())}"
            )
            self.compression = PYARROW_COMPRESSIONS[compression]

            if ipc_exchange is None:
                if worker_type == threading.Thread:
                    ipc_exchange = exchange.Memory()
                elif worker_type == multiprocessing.Process:
                    ipc_exchange = exchange.ArrowIpc()

            if inspect.isclass(ipc_exchange):
                raise TypeError(
                    f"Expected an instance of object, got class definition "
                    f"{ipc_exchange.__class__}, did you forget to instantiate it?"
                )

            if worker_type == threading.Thread:
                if not ipc_exchange.thread_compatible:
                    raise ValueError(
                        f"Exchange {ipc_exchange} is not supported with threading, "
                    )

            if worker_type == multiprocessing.Process:
                if not ipc_exchange.process_compatible:
                    raise ValueError(
                        f"Exchange {ipc_exchange} is not supported with multiprocessing, "
                    )

            self.ipc_exchange = ipc_exchange
        else:  # engine == "rust"
            if compression not in _RUST_COMPRESSION_CHOICES:
                raise AssertionError(
                    f"Compression {compression} not supported, "
                    f"available: {list(_RUST_COMPRESSION_CHOICES)}"
                )
            # Keep the raw string; the Rust extension parses it itself.
            self.compression = compression
            self.ipc_exchange = ipc_exchange

            if ipc_exchange is not None or worker_type is not threading.Thread:
                warnings.warn(
                    "`worker_type` and `ipc_exchange` are ignored when "
                    "`engine=\"rust\"`: the Rust path manages concurrency "
                    "internally and hands batches to Python over the Arrow "
                    "C Data Interface.",
                    stacklevel=2,
                )

        project_id, *_ = source.split(".")
        self.project = project if project is not None else project_id

        logger.debug(
            "Reading with: "
            f"Engine: {self.engine}, "
            f"Project: {self.project}, "
            f"Source: {self.source}, "
            f"Columns: {self.columns}, "
            f"Row restrictions: {self.row_restrictions}, "
            f"Worker count: {self.worker_count}, "
            f"Worker type: {self.worker_type}, "
            f"IPC exchange: {self.ipc_exchange}, "
            f"Batch size: {self.batch_size}, "
            f"Compression: {self.compression}"
        )

    # ------------------------------------------------------------------ enter

    def __enter__(self):
        self.t0 = time.time()
        _bq_table_exists(self.project, self.source)
        if self.engine == "rust":
            return self._enter_rust()
        return self._enter_python()

    def _enter_python(self):
        queue_maxsize = 0
        if type(self.ipc_exchange) is exchange.SharedMemory:
            # Limit in-flight shm segments while the main process drains the queue.
            queue_maxsize = max(4, self.worker_count * 2)
        if self.worker_type == threading.Thread:
            self.queue_results = queue.Queue(maxsize=queue_maxsize)
        else:
            self.queue_results = multiprocessing.Queue(maxsize=queue_maxsize)
        self.read_client = bigquery_storage.BigQueryReadClient()
        self.workers = []

        try:
            self.streams, self.schema = _bq_read_create_streams(
                read_client=self.read_client,
                parent=self.project,
                source=self.source,
                selected_fields=self.columns,
                row_restrictions=self.row_restrictions,
                max_stream_count=self.worker_count * 3,
                compression=self.compression,
            )
        finally:
            _close_client_transport(self.read_client)

        self.workers_done = 0

        assert self.streams, "No streams to read, Table might be empty"

        self.actual_worker_count = min(self.worker_count, len(self.streams))

        logger.debug(
            f"Number of workers: {self.worker_count}, number of streams: {len(self.streams)}"
        )
        logger.debug(f"Actual worker count: {self.actual_worker_count}")

        for read_streams in some_itertools.to_split(self.streams, self.actual_worker_count):
            worker = self.worker_type(
                target=_stream_worker,
                args=(
                    read_streams,
                    self.schema,
                    self.batch_size,
                    self.queue_results,
                    self.ipc_exchange,
                ),
            )
            if self.worker_type == threading.Thread:
                worker.daemon = True
            worker.start()
            self.workers.append(worker)

        return self

    def _enter_rust(self):
        self._rust_reader = _rust.PyReader(
            self.source,
            self.project,
            columns=self.columns,
            row_restrictions=self.row_restrictions,
            max_stream_count=self.worker_count,
            compression=self.compression,
            batch_size=self.batch_size,
        )
        self.schema = pa.ipc.read_schema(pa.py_buffer(self._rust_reader.schema_ipc()))
        self._rust_pending: list[pa.RecordBatch] = []
        self._rust_pending_rows = 0
        self._rust_exhausted = False
        return self

    # ------------------------------------------------------------------- iter

    def __iter__(self):
        return self

    def __next__(self):
        if self.engine == "rust":
            return self._next_rust()
        return self._next_python()

    # --- python iteration ---

    def _process_queue_message(self, kind: str, payload):
        if kind == _QUEUE_DONE:
            self.workers_done += 1
            if self.workers_done == self.actual_worker_count:
                return None
            return "continue"

        if kind == _QUEUE_ERROR:
            error_type, error_message, formatted_traceback = payload
            raise RuntimeError(
                f"BigQuery read worker failed with {error_type}: {error_message}\n"
                f"{formatted_traceback}"
            )

        if kind == _QUEUE_RESULT:
            return payload

        raise RuntimeError(f"Unexpected BigQuery read worker message: {(kind, payload)!r}")

    def _next_python(self):
        while True:
            try:
                element = self.queue_results.get(timeout=_QUEUE_GET_TIMEOUT)
            except queue.Empty:
                self._raise_if_workers_failed()
                continue

            kind, payload = element
            result = self._process_queue_message(kind, payload)
            if result == "continue":
                continue
            if result is None:
                raise StopIteration
            return self.ipc_exchange.load(result)

    def _raise_if_workers_failed(self):
        for idx, worker in enumerate(self.workers):
            exitcode = getattr(worker, "exitcode", None)
            if exitcode not in (None, 0):
                raise RuntimeError(f"BigQuery read worker {idx} exited with code {exitcode}")

        if self.workers_done < self.actual_worker_count and self.workers:
            if all(not worker.is_alive() for worker in self.workers):
                raise RuntimeError(
                    "BigQuery read workers exited before signaling completion"
                )

    # --- rust iteration ---

    def _next_rust(self) -> pa.Table:
        while not self._rust_exhausted and self._rust_pending_rows < self.batch_size:
            try:
                batches = next(self._rust_reader)
            except StopIteration:
                self._rust_exhausted = True
                break
            self._rust_pending.extend(batches)
            self._rust_pending_rows += sum(batch.num_rows for batch in batches)

        if not self._rust_pending:
            raise StopIteration

        if self._rust_exhausted or self._rust_pending_rows <= self.batch_size:
            table = pa.Table.from_batches(self._rust_pending, schema=self.schema)
            self._rust_pending = []
            self._rust_pending_rows = 0
            return table

        combined = pa.Table.from_batches(self._rust_pending, schema=self.schema)
        head = combined.slice(0, self.batch_size)
        tail = combined.slice(self.batch_size)
        self._rust_pending = tail.to_batches()
        self._rust_pending_rows = tail.num_rows
        return head

    # ------------------------------------------------------------------- exit

    def __exit__(self, exc_type, *_):
        if self.engine == "rust":
            self._exit_rust()
        else:
            self._exit_python(exc_type)

        if self.delete_source_on_exit:
            _bq_delete_table(self.project, self.source)

        logger.debug(f"Time taken: {time.time() - self.t0:.2f}")

    def _exit_python(self, exc_type):
        for w in self.workers:
            w.join(timeout=_WORKER_JOIN_TIMEOUT)

        alive_workers = [w for w in self.workers if w.is_alive()]
        for worker in alive_workers:
            terminate = getattr(worker, "terminate", None)
            if terminate:
                terminate()

        for worker in alive_workers:
            if getattr(worker, "terminate", None):
                worker.join(timeout=_WORKER_JOIN_TIMEOUT)

        still_alive = [w for w in self.workers if w.is_alive()]
        if still_alive:
            message = f"{len(still_alive)} BigQuery read worker(s) did not stop cleanly"
            logger.error(message)
            if exc_type is None:
                raise RuntimeError(message)

    def _exit_rust(self):
        # Dropping the Rust reader closes its mpsc channel; the tokio tasks
        # observe the closed receiver and exit.
        self._rust_reader = None


def reader_query(
    project: str,
    query: str,
    *,
    location: str | None = None,
    large_results: bool = False,
    large_results_dataset: str = "_temp_pyarrow_bigquery",
    large_results_expiration_ms: int = 24 * 60 * 60 * 1000,  # 24 hours
    worker_count: int = multiprocessing.cpu_count(),
    worker_type: type[threading.Thread] | type[multiprocessing.Process] = threading.Thread,
    ipc_exchange: exchange.ConcurrencyCompatible | None = None,
    batch_size: int = 100,
    compression: str | None = None,
    engine: Literal["python", "rust"] = "python",
):
    client = bigquery.Client(project=project, location=location)
    try:
        if large_results:
            dataset = bigquery.Dataset(f"{project}.{large_results_dataset}")
            dataset.default_table_expiration_ms = large_results_expiration_ms
            client.create_dataset(dataset, exists_ok=True)

            table = datetime.datetime.now().strftime("%Y%m%d_%H%M%S_%f")

            destination = f"{project}.{large_results_dataset}.{table}"

            job_config = bigquery.QueryJobConfig(destination=destination)
        else:
            job_config = bigquery.QueryJobConfig()

        job = client.query(query, job_config=job_config)
        job.result()

        source = f"{job.destination.project}.{job.destination.dataset_id}.{job.destination.table_id}"  # type: ignore

        return reader(
            source=source,
            project=project,
            worker_count=worker_count,
            worker_type=worker_type,
            ipc_exchange=ipc_exchange,
            batch_size=batch_size,
            compression=compression,
            delete_source_on_exit=large_results,
            engine=engine,
        )
    finally:
        _close_client_transport(client)


def read_table(
    source: str,
    *,
    project: str | None = None,
    columns: list | None = None,
    row_restrictions: str | None = None,
    worker_count: int = multiprocessing.cpu_count(),
    worker_type: type[threading.Thread] | type[multiprocessing.Process] = threading.Thread,
    ipc_exchange: exchange.ConcurrencyCompatible | None = None,
    batch_size: int = 100,
    compression: str | None = None,
    engine: Literal["python", "rust"] = "python",
) -> pa.Table:
    with reader(
        source=source,
        project=project,
        columns=columns,
        row_restrictions=row_restrictions,
        worker_count=worker_count,
        worker_type=worker_type,
        ipc_exchange=ipc_exchange,
        batch_size=batch_size,
        compression=compression,
        engine=engine,
    ) as r:
        return pa.concat_tables(r)


def read_query(
    project: str,
    query: str,
    *,
    location: str | None = None,
    large_results: bool = False,
    large_results_dataset: str = "_temp_pyarrow_bigquery",
    large_results_expiration_ms: int = 24 * 60 * 60 * 1000,  # 24 hours
    worker_count: int = multiprocessing.cpu_count(),
    worker_type: type[threading.Thread] | type[multiprocessing.Process] = threading.Thread,
    ipc_exchange: exchange.ConcurrencyCompatible | None = None,
    batch_size: int = 100,
    compression: str | None = None,
    engine: Literal["python", "rust"] = "python",
):
    with reader_query(
        project=project,
        query=query,
        location=location,
        large_results=large_results,
        large_results_dataset=large_results_dataset,
        large_results_expiration_ms=large_results_expiration_ms,
        worker_count=worker_count,
        worker_type=worker_type,
        ipc_exchange=ipc_exchange,
        batch_size=batch_size,
        compression=compression,
        engine=engine,
    ) as r:
        return pa.concat_tables(r)

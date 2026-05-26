from __future__ import annotations

import datetime
import logging
import multiprocessing
import threading
import warnings

from google.cloud import bigquery
from google.cloud.exceptions import NotFound

import pyarrow as pa

from . import _rust, exchange

logger = logging.getLogger(__name__)

# Recognized values for the `compression` parameter. Validation only — the
# actual codec is selected inside the Rust extension.
_COMPRESSION_CHOICES = (None, "lz4", "zstd")


def _close_client_transport(client):
    if not client:
        return
    transport = getattr(client, "transport", None)
    close = getattr(transport, "close", None)
    if close:
        try:
            close()
        except Exception:
            logger.exception("Failed to close BigQuery client transport")


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


class reader:
    """Context manager that streams a BigQuery table as `pyarrow.Table` chunks.

    Public API is unchanged from the pre-Rust implementation: iteration yields
    `pa.Table` objects with approximately `batch_size` rows each. Internally
    the BigQuery Storage Read session is opened by the Rust extension
    (`pyarrow.bigquery._rust.PyReader`), which manages gRPC, Arrow IPC decode,
    and stream-level concurrency.

    The `worker_type` and `ipc_exchange` parameters are accepted for
    compatibility but are no-ops in the Rust path — concurrency is handled by
    the tokio runtime inside the extension and batches are handed back to
    Python via the Arrow C Data Interface (no IPC roundtrip).
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
    ):
        if compression not in _COMPRESSION_CHOICES:
            raise AssertionError(
                f"Compression {compression} not supported, "
                f"available: {list(_COMPRESSION_CHOICES)}"
            )
        if worker_type not in (threading.Thread, multiprocessing.Process):
            raise ValueError(
                f"Unsupported worker type {worker_type}, "
                f"expected threading.Thread or multiprocessing.Process"
            )

        if ipc_exchange is not None or worker_type is not threading.Thread:
            warnings.warn(
                "`worker_type` and `ipc_exchange` are accepted for API "
                "compatibility but ignored: the Rust read path manages "
                "concurrency internally and hands batches to Python over "
                "the Arrow C Data Interface.",
                stacklevel=2,
            )

        self.source = source
        self.columns = columns
        self.row_restrictions = row_restrictions
        self.worker_count = worker_count
        self.worker_type = worker_type
        self.ipc_exchange = ipc_exchange
        self.batch_size = batch_size
        self.compression = compression
        self.delete_source_on_exit = delete_source_on_exit

        project_id, *_ = source.split(".")
        self.project = project if project is not None else project_id

        logger.debug(
            "Reading with: "
            f"Project: {self.project}, "
            f"Source: {self.source}, "
            f"Columns: {self.columns}, "
            f"Row restrictions: {self.row_restrictions}, "
            f"Worker count: {self.worker_count}, "
            f"Batch size: {self.batch_size}, "
            f"Compression: {self.compression}"
        )

    def __enter__(self):
        _bq_table_exists(self.project, self.source)

        self._rust_reader = _rust.PyReader(
            self.source,
            self.project,
            columns=self.columns,
            row_restrictions=self.row_restrictions,
            max_stream_count=self.worker_count * 3,
            compression=self.compression,
        )
        self.schema = pa.ipc.read_schema(pa.py_buffer(self._rust_reader.schema_ipc()))
        self._pending: list[pa.RecordBatch] = []
        self._pending_rows = 0
        self._exhausted = False
        return self

    def __iter__(self):
        return self

    def __next__(self) -> pa.Table:
        while not self._exhausted and self._pending_rows < self.batch_size:
            try:
                batch = next(self._rust_reader)
            except StopIteration:
                self._exhausted = True
                break
            self._pending.append(batch)
            self._pending_rows += batch.num_rows

        if not self._pending:
            raise StopIteration

        if self._exhausted or self._pending_rows <= self.batch_size:
            table = pa.Table.from_batches(self._pending, schema=self.schema)
            self._pending = []
            self._pending_rows = 0
            return table

        combined = pa.Table.from_batches(self._pending, schema=self.schema)
        head = combined.slice(0, self.batch_size)
        tail = combined.slice(self.batch_size)
        self._pending = tail.to_batches()
        self._pending_rows = tail.num_rows
        return head

    def __exit__(self, exc_type, *_):
        # Dropping the Rust reader closes its mpsc channel; the tokio tasks
        # observe the closed receiver and exit.
        self._rust_reader = None

        if self.delete_source_on_exit:
            _bq_delete_table(self.project, self.source)


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
    ) as r:
        return pa.concat_tables(r)

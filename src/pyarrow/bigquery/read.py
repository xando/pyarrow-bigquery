from __future__ import annotations

import logging
import time
import multiprocessing
import threading
import inspect

from google.cloud import bigquery_storage
from google.cloud import bigquery
from google.cloud.exceptions import NotFound


import pyarrow as pa

from . import some_itertools
from . import exchange


PYARROW_COMPRESSIONS = {
    None: bigquery_storage.ArrowSerializationOptions.CompressionCodec.COMPRESSION_UNSPECIFIED,
    "lz4": bigquery_storage.ArrowSerializationOptions.CompressionCodec.LZ4_FRAME,
    "zstd": bigquery_storage.ArrowSerializationOptions.CompressionCodec.ZSTD,
}

logger = logging.getLogger(__name__)


def _bq_table_exists(project: str, location: str):
    client = bigquery.Client(project=project)

    try:
        client.get_table(location)
        logger.debug(f"Table {location} indeed exists")
    except NotFound as e:
        logger.debug("Table {location} is not found")
        raise e


def _bq_read_create_strems(
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
                queue_results.put(ipc_exchange.store(table[:batch_size]))
                batches = table[batch_size:].to_batches()

        logger.debug(f"Stream {stream} done in {time.time() - t:.2f} seconds")

    if batches:
        queue_key = ipc_exchange.store(pa.Table.from_batches(batches))
        queue_results.put(queue_key)

    queue_results.put(None)


class reader:
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
    ):
        self.source = source
        self.columns = columns
        self.row_restrictions = row_restrictions
        self.worker_count = worker_count
        self.worker_type = worker_type
        self.batch_size = batch_size

        # COMPRESSIONS
        assert compression in PYARROW_COMPRESSIONS, (
            f"Compression {compression} not supported, "
            f"available: {list(PYARROW_COMPRESSIONS.keys())}"
        )
        self.compression = PYARROW_COMPRESSIONS[compression]

        # IPC EXCHANGE
        if ipc_exchange is None:
            if worker_type == threading.Thread:
                ipc_exchange = exchange.Memory()
            elif worker_type == multiprocessing.Process:
                ipc_exchange = exchange.Feather()
            else:
                raise ValueError(
                    f"Unsupported worker type {worker_type}, "
                    f"expected threading.Thread or multiprocessing.Process"
                )

        if inspect.isclass(ipc_exchange):
            raise TypeError(
                f"Expected an instance of object, got class definition {ipc_exchange.__class__}, "
                f" did you forget to instantiate it?"
            )

        if self.worker_type == threading.Thread:
            if not ipc_exchange.thread_compatible:
                raise ValueError(f"Exchange {ipc_exchange} is not supported with threading, ")

        if self.worker_type == multiprocessing.Process:
            if not ipc_exchange.process_compatible:
                raise ValueError(f"Exchange {ipc_exchange} is not supported with multiprocessing, ")

        self.ipc_exchange = ipc_exchange

        # DEFAULT PROJECT from source
        project_id, *_ = source.split(".")

        if project is None:
            self.project = project_id
        else:
            self.project = project

        logger.debug(
            "Reading with: "
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

    def __enter__(self):
        self.t0 = time.time()
        self.manager = multiprocessing.Manager()
        self.queue_results = self.manager.Queue()
        self.read_client = bigquery_storage.BigQueryReadClient()
        self.workers = []

        _bq_table_exists(self.project, self.source)

        self.streams, self.schema = _bq_read_create_strems(
            read_client=self.read_client,
            parent=self.project,
            source=self.source,
            selected_fields=self.columns,
            row_restrictions=self.row_restrictions,
            max_stream_count=self.worker_count * 3,
            compression=self.compression,
        )

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
            worker.start()
            self.workers.append(worker)

        return self

    def __exit__(self, *_, **__):
        for w in self.workers:
            w.join()

        logger.debug(f"Time taken: {time.time() - self.t0:.2f}")

    def __iter__(self):
        return self

    def __next__(self):
        element = self.queue_results.get()
        if not element:
            self.workers_done += 1
            if self.workers_done == self.actual_worker_count:
                raise StopIteration
            else:
                return self.__next__()
        else:
            return self.ipc_exchange.load(element)


def reader_query(
    project: str,
    query: str,
    *,
    worker_count: int = multiprocessing.cpu_count(),
    worker_type: type[threading.Thread] | type[multiprocessing.Process] = threading.Thread,
    ipc_exchange: exchange.ConcurrencyCompatible | None = None,
    batch_size: int = 100,
    compression: str | None = None,
):
    client = bigquery.Client(project=project)
    job = client.query(query)
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
    )


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
    worker_count: int = multiprocessing.cpu_count(),
    worker_type: type[threading.Thread] | type[multiprocessing.Process] = threading.Thread,
    ipc_exchange: exchange.ConcurrencyCompatible | None = None,
    batch_size: int = 100,
    compression: str | None = None,
):
    with reader_query(
        project=project,
        query=query,
        worker_count=worker_count,
        worker_type=worker_type,
        ipc_exchange=ipc_exchange,
        batch_size=batch_size,
        compression=compression,
    ) as r:
        return pa.concat_tables(r)

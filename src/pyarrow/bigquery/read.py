from __future__ import annotations

import logging
import os
import time
import tempfile
import multiprocessing
import threading
import shutil

from google.cloud import bigquery_storage
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

import pyarrow as pa
import pyarrow.feather as fa

from . import some_itertools


logger = logging.getLogger(__name__)


def _bq_table_exists(project: str, location: str):
    client = bigquery.Client(project=project)

    try:
        client.get_table(location)
        logger.debug(f"Table {location} already exists")
    except NotFound as e:
        logger.debug("Table {location} is not found")
        raise e


def _bq_read_create_strems(
    read_client: bigquery_storage.BigQueryReadClient,
    parent: str,
    location: str,
    selected_fields: list | None,
    row_restrictions: str | None,
    max_stream_count: int,
) -> tuple[list[str], pa.Schema]:
    project, dataset, table = location.split(".")

    read_session = bigquery_storage.ReadSession(
        table=f"projects/{project}/datasets/{dataset}/tables/{table}",
        data_format=bigquery_storage.DataFormat.ARROW,
        read_options={
            "selected_fields": selected_fields,
            "row_restriction": row_restrictions,
        },
    )

    read_session = read_client.create_read_session(
        parent=f"projects/{parent}",
        read_session=read_session,
        max_stream_count=max_stream_count,
    )

    schema_buffer = pa.py_buffer(read_session.arrow_schema.serialized_schema)
    schema = pa.ipc.read_schema(schema_buffer)

    return read_session.streams, schema


def _stream_worker(read_client, read_streams, table_schema, batch_size, queue_results, temp_dir):
    batches = []

    for stream in read_streams:
        t = time.time()

        for message in read_client.read_rows(stream.name):
            record_batch = pa.ipc.read_record_batch(message.arrow_record_batch.serialized_record_batch, table_schema)

            batches.append(record_batch)

            if sum(b.num_rows for b in batches) >= batch_size:
                table = pa.Table.from_batches(batches)

                element = tempfile.mktemp(dir=temp_dir)
                fa.write_feather(table[:batch_size], element)
                queue_results.put(element)

                batches = table[batch_size:].to_batches()

        logger.debug(f"Stream {stream.name} done in {time.time() - t:.2f} seconds")

    if batches:
        table = pa.Table.from_batches(batches)
        element = tempfile.mktemp(dir=temp_dir)
        fa.write_feather(table, element)
        queue_results.put(element)

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
        batch_size: int = 100,
    ):
        self.source = source
        self.project = project
        self.columns = columns
        self.row_restrictions = row_restrictions
        self.worker_count = worker_count
        self.worker_type = worker_type
        self.batch_size = batch_size

        project_id, *_ = source.split(".")

        if not project:
            self.project = project_id

    def __enter__(self):
        self.t0 = time.time()

        self.queue_results = multiprocessing.Queue()
        self.read_client = bigquery_storage.BigQueryReadClient()
        self.temp_dir = tempfile.mkdtemp()
        self.workers = []

        _bq_table_exists(self.project, self.source)

        self.streams, self.schema = _bq_read_create_strems(
            read_client=self.read_client,
            parent=self.project,
            location=self.source,
            selected_fields=self.columns,
            row_restrictions=self.row_restrictions,
            max_stream_count=self.worker_count * 3,
        )
        self.workers_done = 0

        assert self.streams, "No streams to read, Table might be empty"

        self.actual_worker_count = min(self.worker_count, len(self.streams))

        logger.debug(f"Number of workers: {self.worker_count}, number of streams: {len(self.streams)}")
        logger.debug(f"Actual worker count: {self.actual_worker_count}")

        for worker_streams in some_itertools.to_split(self.streams, self.actual_worker_count):
            worker = self.worker_type(
                target=_stream_worker,
                args=(
                    self.read_client,
                    worker_streams,
                    self.schema,
                    self.batch_size,
                    self.queue_results,
                    self.temp_dir,
                ),
            )
            worker.start()
            self.workers.append(worker)

        return self

    def __exit__(self, *_, **__):
        for w in self.workers:
            w.join()

        shutil.rmtree(self.temp_dir, ignore_errors=True)
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
            table = fa.read_table(element)
            os.remove(element)
            return table


def reader_query(
    project: str,
    query: str,
    *,
    worker_count: int = multiprocessing.cpu_count(),
    worker_type: type[threading.Thread] | type[multiprocessing.Process] = threading.Thread,
    batch_size: int = 100,
):
    client = bigquery.Client(project=project)
    job = client.query(query)
    job.result()

    source = f"{job.destination.project}.{job.destination.dataset_id}.{job.destination.table_id}"

    return reader(
        source=source,
        project=project,
        worker_count=worker_count,
        worker_type=worker_type,
        batch_size=batch_size,
    )


def read_table(
    source: str,
    *,
    project: str | None = None,
    columns: list | None = None,
    row_restrictions: str | None = None,
    worker_count: int = multiprocessing.cpu_count(),
    worker_type: type[threading.Thread] | type[multiprocessing.Process] = threading.Thread,
    batch_size: int = 100,
):
    with reader(
        source=source,
        project=project,
        columns=columns,
        row_restrictions=row_restrictions,
        worker_count=worker_count,
        worker_type=worker_type,
        batch_size=batch_size,
    ) as r:
        return pa.concat_tables(r)


def read_query(
    project: str,
    query: str,
    *,
    worker_count: int = multiprocessing.cpu_count(),
    worker_type: type[threading.Thread] | type[multiprocessing.Process] = threading.Thread,
    batch_size: int = 100,
):
    with reader_query(
        project=project, query=query, worker_count=worker_count, worker_type=worker_type, batch_size=batch_size
    ) as r:
        return pa.concat_tables(r)

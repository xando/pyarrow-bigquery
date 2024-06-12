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

        logger.debug(f"Stream {stream.name} done in {time.time()-t:.2f} seconds")

    if batches:
        table = pa.Table.from_batches(batches)
        element = tempfile.mktemp(dir=temp_dir)
        fa.write_feather(table, element)
        queue_results.put(element)

    queue_results.put(None)


def reader(
    source: str,
    *,
    project: str | None = None,
    columns: list | None = None,
    row_restrictions: str | None = None,
    worker_count: int = multiprocessing.cpu_count(),
    worker_type: type[threading.Thread] | type[multiprocessing.Process] = threading.Thread,
    batch_size: int = 100,
):
    t0 = time.time()
    project_id, *_ = source.split(".")

    if not project:
        project = project_id

    queue_results = multiprocessing.Queue()
    read_client = bigquery_storage.BigQueryReadClient()

    _bq_table_exists(project, source)

    streams, streams_schema = _bq_read_create_strems(
        read_client=read_client,
        parent=project,
        location=source,
        selected_fields=columns,
        row_restrictions=row_restrictions,
        max_stream_count=worker_count * 3,
    )
    workers_done = 0

    assert streams, "No streams to read, Table might be empty"

    logger.debug(f"Number of workers: {worker_count}, number of streams: {len(streams)}")

    actual_worker_count = min(worker_count, len(streams))

    logger.debug(f"Actual worker count: {actual_worker_count}")

    temp_dir = tempfile.mkdtemp()

    try:
        for streams in some_itertools.to_split(streams, actual_worker_count):
            e = worker_type(
                target=_stream_worker,
                args=(
                    read_client,
                    streams,
                    streams_schema,
                    batch_size,
                    queue_results,
                    temp_dir,
                ),
            )
            e.start()

        while True:
            element = queue_results.get()

            if not element:
                workers_done += 1

                if workers_done == actual_worker_count:
                    break
            else:
                table = fa.read_table(element)
                os.remove(element)
                yield table
    finally:
        t = time.time()
        shutil.rmtree(temp_dir, ignore_errors=True)
        logger.debug(f"Time to cleanup temp directory: {time.time()-t:.2f}")
        logger.debug(f"Time taken to read: {time.time()-t0:.2f}")


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
    return pa.concat_tables(
        reader(
            source=source,
            project=project,
            columns=columns,
            row_restrictions=row_restrictions,
            worker_count=worker_count,
            worker_type=worker_type,
            batch_size=batch_size,
        )
    )


def read_query(
    project: str,
    query: str,
    *,
    worker_count: int = multiprocessing.cpu_count(),
    worker_type: type[threading.Thread] | type[multiprocessing.Process] = threading.Thread,
    batch_size: int = 100,
):
    return pa.concat_tables(
        reader_query(
            project=project, query=query, worker_count=worker_count, worker_type=worker_type, batch_size=batch_size
        )
    )

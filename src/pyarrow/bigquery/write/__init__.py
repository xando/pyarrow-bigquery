from __future__ import annotations

import os
import time
import datetime
import multiprocessing
import threading
import shutil
import tempfile
import logging
import collections


from google.cloud import bigquery
from google.cloud import bigquery_storage_v1
from google.cloud.bigquery_storage_v1.writer import AppendRowsStream
from google.protobuf import descriptor_pb2
from google.api_core.exceptions import Unknown, NotFound
from google.api_core import retry

import pyarrow as pa
import pyarrow.feather as fa

from . import pa_to_bq
from . import pa_to_pb
from . import upload
from .. import some_itertools

logger = logging.getLogger(__name__)


Stream = collections.namedtuple("Stream", ["append_rows_stream", "write_stream"])


def _bq_create_table(*, project, location, schema, expire, overwrite):
    client = bigquery.Client(project=project)

    if overwrite:
        client.delete_table(location, not_found_ok=True)

    bq_schema = pa_to_bq.generate(schema)

    table = bigquery.Table(location, schema=bq_schema)

    client.create_table(table)

    if expire:
        table.expires = datetime.datetime.now() + datetime.timedelta(seconds=expire)
        client.update_table(table, ["expires"])

    logger.debug(f"Created BigQuery table '{location}'")


def _bq_write_create_stream(write_client: bigquery_storage_v1.BigQueryWriteClient, parent, protobuf_definition):
    write_stream = write_client.create_write_stream(
        parent=parent,
        write_stream=bigquery_storage_v1.types.WriteStream(type=bigquery_storage_v1.types.WriteStream.Type.PENDING),
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


def _bq_storage_close_stream(write_client, stream, parent):
    stream.append_rows_stream.close()
    write_client.finalize_write_stream(name=stream.write_stream.name)

    batch_commit_write_streams_request = bigquery_storage_v1.types.BatchCommitWriteStreamsRequest()
    batch_commit_write_streams_request.parent = parent
    batch_commit_write_streams_request.write_streams = [stream.write_stream.name]

    write_client.batch_commit_write_streams(batch_commit_write_streams_request)

    logger.debug(f"Stream '{stream.write_stream.name}' closed")


def _stream_worker(
    write_client: bigquery_storage_v1.BigQueryWriteClient,
    parent: str,
    schema_protobuf,
    queue_results,
):
    stream = _bq_write_create_stream(write_client, parent, schema_protobuf)

    offset = 0

    while True:
        element = queue_results.get()
        if element is None:
            break

        table = fa.read_table(element)

        upload.upload_data(stream, table, schema_protobuf, offset)

        os.remove(element)

        offset += table.num_rows

    _bq_storage_close_stream(write_client, stream, parent)


class writer:
    """Method to handle"""

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
        batch_size: int = 100,
    ):
        self.project = project
        self.where = where
        self.schema = schema

        self.table_create = table_create
        self.table_expire = table_expire
        self.table_overwrite = table_overwrite

        self.worker_count = worker_count
        self.worker_type = worker_type

        self.batch_size = batch_size

        project_id, dataset_id, table_id = where.split(".")

        self.parent = f"projects/{project_id}/datasets/{dataset_id}/tables/{table_id}"

        if not self.project:
            self.project = project_id

    def __enter__(self):
        self.t0 = time.time()
        self.temp_dir = tempfile.mkdtemp()
        self.schema_protobuf = pa_to_pb.generate(self.schema)

        if self.table_create:
            _bq_create_table(
                project=self.project,
                location=self.where,
                schema=self.schema,
                expire=self.table_expire,
                overwrite=self.table_overwrite,
            )

        self.queue_results = multiprocessing.Queue()
        self.workers = []
        write_client = bigquery_storage_v1.BigQueryWriteClient()

        for _ in range(self.worker_count):
            worker = self.worker_type(
                target=_stream_worker,
                args=(
                    write_client,
                    self.parent,
                    self.schema_protobuf,
                    self.queue_results,
                ),
            )
            worker.start()
            self.workers.append(worker)

        return self

    def write_table(self, table):
        for table_chunk in some_itertools.to_chunks(table, self.batch_size):
            element = tempfile.mktemp(dir=self.temp_dir)
            fa.write_feather(table_chunk, element)
            self.queue_results.put(element)

    def write_batch(self, batch):
        element = tempfile.mktemp(dir=self.temp_dir)
        fa.write_feather(pa.Table.from_batches([batch]), element)
        self.queue_results.put(element)

    def __exit__(self, *_, **__):
        for _ in range(self.worker_count):
            self.queue_results.put(None)

        for w in self.workers:
            w.join()

        shutil.rmtree(self.temp_dir, ignore_errors=True)

        logger.debug(f"Time taken: {time.time() - self.t0}")


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
    batch_size: int = 100,
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
        batch_size=batch_size,
    ) as w:
        for table_chunk in some_itertools.to_split(table, w.worker_count):
            w.write_table(table_chunk)

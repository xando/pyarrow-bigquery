import importlib

import grpc
import pyarrow as pa
import pytest
from google.auth.credentials import AnonymousCredentials


def test_stream_worker_reports_errors(monkeypatch):
    write_mod = importlib.import_module("pyarrow.bigquery.write")

    class FailingWriteClient:
        def create_write_stream(self, *args, **kwargs):
            raise RuntimeError("failed to create stream")

    monkeypatch.setattr(
        write_mod.bigquery_storage_v1, "BigQueryWriteClient", FailingWriteClient
    )

    queue_results = write_mod.queue.Queue()
    queue_worker_status = write_mod.queue.Queue()
    write_mod._stream_worker("projects/p/datasets/d/tables/t", object(), queue_results, queue_worker_status)

    kind, payload = queue_worker_status.get(timeout=1)

    assert kind == write_mod._QUEUE_ERROR
    assert payload[0] == "RuntimeError"
    assert "failed to create stream" in payload[1]


def test_writer_surfaces_worker_errors(monkeypatch):
    write_mod = importlib.import_module("pyarrow.bigquery.write")

    def failing_stream_worker(
        parent, schema_protobuf, queue_results, queue_worker_status,
        compression=None, stop_event=None,
    ):
        queue_worker_status.put(
            write_mod._queue_error(RuntimeError("upload failed"))
        )

    monkeypatch.setattr(write_mod, "_stream_worker", failing_stream_worker)
    monkeypatch.setattr(write_mod, "_bq_create_table", lambda **kwargs: None)
    monkeypatch.setattr(write_mod.pa_to_pb, "generate", lambda schema: object())

    schema = pa.schema([("x", pa.int64())])
    table = pa.table({"x": [1]})

    with pytest.raises(RuntimeError, match="upload failed"):
        with write_mod.writer(schema, "project.dataset.table", worker_count=1) as w:
            w.write_table(table)


def test_writer_validates_worker_type():
    write_mod = importlib.import_module("pyarrow.bigquery.write")
    schema = pa.schema([("x", pa.int64())])
    with pytest.raises(ValueError, match="Unsupported worker type"):
        # Deliberately pass an invalid type to exercise runtime validation.
        write_mod.writer(
            schema, "project.dataset.table", worker_type=object  # ty: ignore[invalid-argument-type]
        )


@pytest.mark.parametrize("write_method", ["write_table", "write_batch"])
@pytest.mark.parametrize("compression", [None, "gzip"])
def test_writer_passes_complete_feather_files_to_workers(monkeypatch, write_method, compression):
    write_mod = importlib.import_module("pyarrow.bigquery.write")
    uploaded = []
    worker_compressions = []

    def fake_stream_worker(
        parent, schema_protobuf, queue_results, queue_worker_status,
        compression=None, stop_event=None,
    ):
        worker_compressions.append(compression)
        while (path := queue_results.get()) is not None:
            uploaded.append(write_mod.fa.read_table(path))
            write_mod.os.remove(path)
        queue_worker_status.put((write_mod._QUEUE_DONE, None))

    monkeypatch.setattr(write_mod, "_stream_worker", fake_stream_worker)
    monkeypatch.setattr(write_mod, "_bq_create_table", lambda **kwargs: None)
    monkeypatch.setattr(write_mod.pa_to_pb, "generate", lambda schema: object())

    table = pa.table({"x": [1, 2, 3]})
    data = table if write_method == "write_table" else table.to_batches()[0]
    with write_mod.writer(
        table.schema, "project.dataset.table", worker_count=1, compression=compression
    ) as w:
        getattr(w, write_method)(data)

    assert len(uploaded) == 1
    assert uploaded[0].equals(table)
    assert worker_compressions == [compression]
    assert not write_mod.os.path.exists(w.temp_dir)


def test_writer_uses_bounded_queue(monkeypatch):
    write_mod = importlib.import_module("pyarrow.bigquery.write")

    def fake_stream_worker(
        parent, schema_protobuf, queue_results, queue_worker_status,
        compression=None, stop_event=None,
    ):
        queue_worker_status.put((write_mod._QUEUE_DONE, None))

    monkeypatch.setattr(write_mod, "_stream_worker", fake_stream_worker)
    monkeypatch.setattr(write_mod, "_bq_create_table", lambda **kwargs: None)
    monkeypatch.setattr(write_mod.pa_to_pb, "generate", lambda schema: object())

    schema = pa.schema([("x", pa.int64())])
    with write_mod.writer(schema, "project.dataset.table", worker_count=2) as w:
        assert w.queue_results.maxsize == 4


def test_stream_worker_closes_client_transport(monkeypatch):
    write_mod = importlib.import_module("pyarrow.bigquery.write")

    closed = []

    class FakeTransport:
        def close(self):
            closed.append(True)

    class FakeClient:
        transport = FakeTransport()

    monkeypatch.setattr(write_mod.bigquery_storage_v1, "BigQueryWriteClient", lambda: FakeClient())
    monkeypatch.setattr(
        write_mod,
        "_bq_write_create_stream",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("boom")),
    )

    queue_results = write_mod.queue.Queue()
    queue_worker_status = write_mod.queue.Queue()
    write_mod._stream_worker("projects/p/datasets/d/tables/t", object(), queue_results, queue_worker_status)

    assert closed == [True]


def test_gzip_worker_configures_transport_uploads_and_closes(monkeypatch, tmp_path):
    write_mod = importlib.import_module("pyarrow.bigquery.write")
    transport_cls = write_mod.BigQueryWriteGrpcTransport
    create_channel = transport_cls.create_channel
    close_transport = transport_cls.close
    channel_options = []
    uploaded = []
    closed = []

    def compressed_channel(**kwargs):
        channel_options.append(kwargs)
        # Creating a channel does not connect to the service. No credentials or
        # network are used; the upload and finalization below stay local.
        return create_channel(credentials=AnonymousCredentials(), **kwargs)

    def close(self):
        closed.append(self)
        return close_transport(self)

    def create_stream(client, parent, schema):
        assert isinstance(client.transport, transport_cls)
        return object()

    monkeypatch.setattr(transport_cls, "create_channel", compressed_channel)
    monkeypatch.setattr(transport_cls, "close", close)
    monkeypatch.setattr(write_mod, "_bq_write_create_stream", create_stream)
    monkeypatch.setattr(write_mod, "_bq_storage_close_stream", lambda *args: None)
    monkeypatch.setattr(
        write_mod.upload, "upload_data",
        lambda stream, table, schema, offset: uploaded.append((table, offset)),
    )
    table = pa.table({"value": ["repeat" * 100] * 3})
    path = tmp_path / "batch.feather"
    write_mod.fa.write_feather(table, path)
    results = write_mod.queue.Queue()
    status = write_mod.queue.Queue()
    results.put(str(path))
    results.put(None)

    write_mod._stream_worker("projects/p/datasets/d/tables/t", object(), results, status, "gzip")

    assert status.get_nowait() == (write_mod._QUEUE_DONE, None)
    assert channel_options == [{
        "compression": grpc.Compression.Gzip,
        "options": [
            ("grpc.max_send_message_length", -1),
            ("grpc.max_receive_message_length", -1),
        ],
    }]
    assert len(uploaded) == 1
    assert uploaded[0][0].equals(table)
    assert uploaded[0][1] == 0
    assert not path.exists()
    assert len(closed) == 1

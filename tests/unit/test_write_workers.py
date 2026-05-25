import importlib

import pyarrow as pa
import pytest


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

    def failing_stream_worker(parent, schema_protobuf, queue_results, queue_worker_status):
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
        write_mod.writer(schema, "project.dataset.table", worker_type=object)


def test_writer_uses_bounded_queue(monkeypatch):
    write_mod = importlib.import_module("pyarrow.bigquery.write")

    def fake_stream_worker(parent, schema_protobuf, queue_results, queue_worker_status):
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

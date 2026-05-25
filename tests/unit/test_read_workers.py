import importlib

import pyarrow as pa
import pytest


def test_thread_reader_uses_thread_queue_without_manager(monkeypatch):
    read_mod = importlib.import_module("pyarrow.bigquery.read")

    def fail_manager():
        raise AssertionError("multiprocessing.Manager should not be used in thread mode")

    class FakeReadClient:
        pass

    def fake_stream_worker(read_streams, table_schema, batch_size, queue_results, ipc_exchange):
        table = pa.table({"x": [1]})
        queue_results.put(read_mod._queue_result(ipc_exchange.store(table)))
        queue_results.put((read_mod._QUEUE_DONE, None))

    monkeypatch.setattr(read_mod.multiprocessing, "Manager", fail_manager)
    monkeypatch.setattr(read_mod.bigquery_storage, "BigQueryReadClient", FakeReadClient)
    monkeypatch.setattr(read_mod, "_bq_table_exists", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        read_mod,
        "_bq_read_create_streams",
        lambda **kwargs: (["stream-1"], pa.schema([("x", pa.int64())])),
    )
    monkeypatch.setattr(read_mod, "_stream_worker", fake_stream_worker)

    with read_mod.reader("project.dataset.table", worker_count=1) as r:
        assert next(r).to_pydict() == {"x": [1]}
        with pytest.raises(StopIteration):
            next(r)


def test_stream_worker_reports_errors(monkeypatch):
    read_mod = importlib.import_module("pyarrow.bigquery.read")

    class FailingReadClient:
        def read_rows(self, stream):
            raise RuntimeError(f"failed to read {stream}")

    monkeypatch.setattr(read_mod.bigquery_storage, "BigQueryReadClient", FailingReadClient)

    queue_results = read_mod.queue.Queue()
    read_mod._stream_worker(
        ["stream-1"],
        pa.schema([("x", pa.int64())]),
        100,
        queue_results,
        read_mod.exchange.Memory(),
    )

    kind, payload = queue_results.get(timeout=1)

    assert kind == read_mod._QUEUE_ERROR
    assert payload[0] == "RuntimeError"
    assert "failed to read stream-1" in payload[1]

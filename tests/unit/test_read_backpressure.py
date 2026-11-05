import gc
import importlib
import multiprocessing
import queue
import tempfile
import threading
import weakref
from types import SimpleNamespace

import pyarrow as pa
import pytest

from pyarrow.bigquery import exchange

read_mod = importlib.import_module("pyarrow.bigquery.read")


class OfflineReadClient:
    def read_rows(self, stream):
        for value in range(100):
            batch = pa.record_batch([[value]], names=["x"])
            yield SimpleNamespace(arrow_record_batch=SimpleNamespace(
                serialized_record_batch=batch.serialize(),
            ))


@pytest.fixture
def offline_read(monkeypatch):
    monkeypatch.setattr(read_mod, "_bq_table_exists", lambda *args: None)
    monkeypatch.setattr(read_mod.bigquery_storage, "BigQueryReadClient", OfflineReadClient)
    monkeypatch.setattr(read_mod, "_bq_read_create_streams", lambda **kwargs: (
        ["stream"], pa.schema([("x", pa.int64())]),
    ))


def test_memory_load_transfers_ownership():
    ipc = exchange.Memory()
    table = pa.table({"x": [1, 2, 3]})
    ref = weakref.ref(table)
    key = ipc.store(table)
    loaded = ipc.load(key)
    assert loaded is table
    assert ipc.memory == {}
    del loaded, table
    gc.collect()
    assert ref() is None


@pytest.mark.parametrize("exchange_type", [exchange.Memory, exchange.ArrowIpc, exchange.Feather])
@pytest.mark.parametrize("body_error", [False, True])
def test_bounded_read_ahead_cancels_and_discards_unread_results(
    offline_read, monkeypatch, tmp_path, exchange_type, body_error,
):
    monkeypatch.setattr(tempfile, "tempdir", str(tmp_path))
    blocked = threading.Event()

    class RecordingExchange(exchange_type):
        stores = 0

        def store(self, table):
            key = super().store(table)
            self.stores += 1
            if self.stores == 5:  # four queued plus one held by the producer
                blocked.set()
            return key

    ipc = RecordingExchange()
    original_error = ValueError("consumer failed")
    try:
        with read_mod.reader("p.d.t", worker_count=1, batch_size=1, ipc_exchange=ipc) as reader:
            assert reader.queue_results.maxsize == 4
            assert blocked.wait(timeout=5)
            assert ipc.stores == 5
            if body_error:
                raise original_error
    except ValueError as error:
        assert body_error and error is original_error
    assert not any(worker.is_alive() for worker in reader.workers)
    assert list(tmp_path.iterdir()) == []
    if isinstance(ipc, exchange.Memory):
        assert ipc.memory == {}


def test_normal_iteration_delivers_every_row_and_stays_exhausted(offline_read):
    ipc = exchange.Memory()
    with read_mod.reader("p.d.t", worker_count=1, batch_size=1, ipc_exchange=ipc) as reader:
        assert pa.concat_tables(reader).to_pydict() == {"x": list(range(100))}
        with pytest.raises(StopIteration):
            next(reader)
        with pytest.raises(StopIteration):
            next(reader)
        assert ipc.memory == {}


def test_error_on_bounded_worker_queue_is_delivered(offline_read, monkeypatch):
    class FailingClient(OfflineReadClient):
        def read_rows(self, stream):
            yield from super().read_rows(stream)
            raise RuntimeError("read failed")

    monkeypatch.setattr(read_mod.bigquery_storage, "BigQueryReadClient", FailingClient)
    ipc = exchange.Memory()
    with pytest.raises(RuntimeError, match="read failed"):
        with read_mod.reader("p.d.t", worker_count=1, batch_size=1, ipc_exchange=ipc) as reader:
            list(reader)
    assert ipc.memory == {}
    assert not any(worker.is_alive() for worker in reader.workers)


def _process_worker(results, stop, blocked, directory):
    # The real worker and queue protocol, with only the network client replaced.
    tempfile.tempdir = directory
    read_mod.bigquery_storage.BigQueryReadClient = OfflineReadClient

    class RecordingIpc(exchange.ArrowIpc):
        stores = 0

        def store(self, table):
            key = super().store(table)
            self.stores += 1
            if self.stores == 2:
                blocked.set()
            return key

    read_mod._stream_worker(
        ["stream"], pa.schema([("x", pa.int64())]), 1, results, RecordingIpc(), stop,
    )


def test_spawn_worker_cancels_when_result_queue_is_full(tmp_path):
    context = multiprocessing.get_context("spawn")
    results = context.Queue(maxsize=1)
    stop, blocked = context.Event(), context.Event()
    process = context.Process(target=_process_worker, args=(results, stop, blocked, str(tmp_path)))
    process.start()
    try:
        assert blocked.wait(timeout=10)
        stop.set()
        process.join(timeout=5)
        assert not process.is_alive()
        assert process.exitcode == 0
        while True:
            try:
                kind, key = results.get(timeout=0.1)
            except queue.Empty:
                break
            if kind == read_mod._QUEUE_RESULT:
                exchange.ArrowIpc().discard(key)
        assert list(tmp_path.iterdir()) == []
    finally:
        if process.is_alive():
            process.terminate()
            process.join(timeout=5)
        results.close()
        results.join_thread()


def test_shared_memory_discard_unlinks_without_loading():
    ipc = exchange.SharedMemory()
    key = ipc.store(pa.table({"x": [1]}))
    ipc.discard(key)
    with pytest.raises(FileNotFoundError):
        ipc.load(key)

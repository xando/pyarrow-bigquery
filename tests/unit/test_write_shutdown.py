import importlib
import multiprocessing
import queue
import threading
import time
from concurrent.futures import Future
from concurrent.futures import TimeoutError as FutureTimeoutError
from types import SimpleNamespace

import pyarrow as pa
import pytest

write_mod = importlib.import_module("pyarrow.bigquery.write")
TABLE = pa.table({"x": [1]})


def _slow_worker(parent, schema, work, status, compression=None, stop_event=None):
    while not stop_event.is_set():
        try:
            path = work.get(timeout=0.01)
        except queue.Empty:
            continue
        if path is None:
            # Longer than the join timeout used by the regression test.
            time.sleep(0.2)
            status.put((write_mod._QUEUE_DONE, "slow-stream"))
            return
        write_mod.os.remove(path)


def _waiting_worker(parent, schema, work, status, compression=None, stop_event=None):
    stop_event.wait(timeout=10)


def _crashing_worker(parent, schema, work, status, compression=None, stop_event=None):
    write_mod.os._exit(7)


@pytest.fixture
def offline_writer(monkeypatch):
    commits = []
    # Keep process arguments picklable; protobuf descriptor portability is a
    # separate issue. These fake workers never serialize or contact BigQuery.
    monkeypatch.setattr(write_mod.pa_to_pb, "generate", lambda schema: b"offline-schema")
    monkeypatch.setattr(write_mod, "_bq_create_table", lambda **kwargs: None)
    monkeypatch.setattr(write_mod, "_bq_storage_commit_streams",
                        lambda parent, names: commits.append(list(names)))
    monkeypatch.setattr(write_mod, "_QUEUE_GET_TIMEOUT", 0.01)
    return commits


@pytest.mark.parametrize("worker_type", [threading.Thread, multiprocessing.Process])
def test_healthy_upload_is_not_limited_by_join_timeout(offline_writer, monkeypatch, worker_type):
    monkeypatch.setattr(write_mod, "_stream_worker", _slow_worker)
    monkeypatch.setattr(write_mod, "_WORKER_JOIN_TIMEOUT", 0.05)
    with write_mod.writer(TABLE.schema, "p.d.t", worker_count=1, worker_type=worker_type) as writer:
        writer.write_table(TABLE)
    assert offline_writer == [["slow-stream"]]
    assert not any(worker.is_alive() for worker in writer.workers)
    assert not write_mod.os.path.exists(writer.temp_dir)


@pytest.mark.parametrize("operation", ["write_table", "write_batch", "exit"])
def test_full_queue_detects_worker_failure_without_deadlock(offline_writer, monkeypatch, operation):
    fail, full, finished = threading.Event(), threading.Event(), threading.Event()
    errors = []

    def worker(parent, schema, work, status, compression=None, stop_event=None):
        assert fail.wait(timeout=5)
        status.put((write_mod._QUEUE_ERROR, ("RuntimeError", "worker failed", "traceback")))

    monkeypatch.setattr(write_mod, "_stream_worker", worker)
    writer = write_mod.writer(TABLE.schema, "p.d.t", worker_count=1)
    writer.__enter__()
    original_put = writer.queue_results.put

    def put(item, *args, **kwargs):
        if writer.queue_results.full():
            full.set()
        return original_put(item, *args, **kwargs)

    monkeypatch.setattr(writer.queue_results, "put", put)
    for _ in range(writer.queue_results.maxsize):
        writer.write_table(TABLE)

    def invoke():
        try:
            if operation == "exit":
                writer.__exit__(None, None, None)
            else:
                data = TABLE if operation == "write_table" else TABLE.to_batches()[0]
                getattr(writer, operation)(data)
        except BaseException as error:
            errors.append(error)
        finally:
            finished.set()

    producer = threading.Thread(target=invoke, daemon=True)
    producer.start()
    try:
        assert full.wait(timeout=5)
        fail.set()
        assert finished.wait(timeout=5), "producer/shutdown is blocked behind a failed worker"
        assert len(errors) == 1
        assert "worker failed" in str(errors[0])
        if operation != "exit":
            with pytest.raises(RuntimeError, match="worker failed") as raised:
                writer.__exit__(None, None, None)
            assert raised.value is errors[0]
        assert offline_writer == []
        assert not write_mod.os.path.exists(writer.temp_dir)
    finally:
        fail.set()
        # Release a regressed blocking put so a failed test cannot hang pytest.
        while not writer.queue_results.empty():
            writer.queue_results.get_nowait()
        producer.join(timeout=5)
        if writer._python_open:
            writer.__exit__(ValueError, None, None)


def test_body_exception_cancels_full_queue_without_sending_sentinels(offline_writer, monkeypatch):
    monkeypatch.setattr(write_mod, "_stream_worker", _waiting_worker)
    error = ValueError("consumer failed")
    with pytest.raises(ValueError) as raised:
        with write_mod.writer(TABLE.schema, "p.d.t", worker_count=1) as writer:
            for _ in range(writer.queue_results.maxsize):
                writer.write_table(TABLE)

            def unexpected_put(*args, **kwargs):
                pytest.fail("aborting must not enqueue sentinels into a full queue")

            monkeypatch.setattr(writer.queue_results, "put", unexpected_put)
            raise error
    assert raised.value is error
    assert offline_writer == []
    assert not any(worker.is_alive() for worker in writer.workers)
    assert not write_mod.os.path.exists(writer.temp_dir)


def test_abrupt_process_exit_is_detected_with_a_full_queue(offline_writer, monkeypatch):
    monkeypatch.setattr(write_mod, "_stream_worker", _crashing_worker)
    with pytest.raises(RuntimeError, match="exited with code 7"):
        with write_mod.writer(TABLE.schema, "p.d.t", worker_count=1,
                              worker_type=multiprocessing.Process) as writer:
            for _ in range(writer.queue_results._maxsize):
                writer.write_table(TABLE)
    assert offline_writer == []
    assert not any(worker.is_alive() for worker in writer.workers)
    assert not write_mod.os.path.exists(writer.temp_dir)


def test_silent_worker_exit_is_detected(offline_writer, monkeypatch):
    monkeypatch.setattr(write_mod, "_stream_worker", lambda *args: None)
    with pytest.raises(RuntimeError, match="exited before signaling completion"):
        with write_mod.writer(TABLE.schema, "p.d.t", worker_count=1) as writer:
            writer.workers[0].join(timeout=1)
            writer.write_table(TABLE)
    assert offline_writer == []


@pytest.mark.parametrize("method", ["write_table", "write_batch"])
def test_writes_require_an_open_context(offline_writer, monkeypatch, method):
    monkeypatch.setattr(write_mod, "_stream_worker", _slow_worker)
    writer = write_mod.writer(TABLE.schema, "p.d.t", worker_count=1)
    data = TABLE if method == "write_table" else TABLE.to_batches()[0]
    with pytest.raises(RuntimeError, match="writer is not open"):
        getattr(writer, method)(data)
    with writer:
        getattr(writer, method)(data)
    with pytest.raises(RuntimeError, match="writer is not open"):
        getattr(writer, method)(data)


def test_setup_failure_cleans_temporary_directory(offline_writer, monkeypatch):
    error = RuntimeError("table creation failed")

    def fail(**kwargs):
        raise error

    monkeypatch.setattr(write_mod, "_bq_create_table", fail)
    writer = write_mod.writer(TABLE.schema, "p.d.t", worker_count=1)
    with pytest.raises(RuntimeError) as raised:
        writer.__enter__()
    assert raised.value is error
    assert not write_mod.os.path.exists(writer.temp_dir)
    assert not writer._python_open


def test_partial_startup_failure_stops_started_workers(offline_writer, monkeypatch):
    monkeypatch.setattr(write_mod, "_stream_worker", _waiting_worker)
    start = threading.Thread.start
    starts = []
    error = RuntimeError("cannot start worker")

    def fail_second(thread):
        if starts:
            raise error
        starts.append(thread)
        start(thread)

    monkeypatch.setattr(threading.Thread, "start", fail_second)
    writer = write_mod.writer(TABLE.schema, "p.d.t", worker_count=2)
    with pytest.raises(RuntimeError) as raised:
        writer.__enter__()
    assert raised.value is error
    assert not starts[0].is_alive()
    assert not write_mod.os.path.exists(writer.temp_dir)
    assert offline_writer == []


def test_caught_serialization_error_still_prevents_commit(offline_writer, monkeypatch):
    monkeypatch.setattr(write_mod, "_stream_worker", _waiting_worker)
    error = RuntimeError("cannot write Feather")

    def fail(*args):
        raise error

    monkeypatch.setattr(write_mod.fa, "write_feather", fail)
    with pytest.raises(RuntimeError) as raised:
        with write_mod.writer(TABLE.schema, "p.d.t", worker_count=1) as writer:
            with pytest.raises(RuntimeError) as first:
                writer.write_table(TABLE)
            assert first.value is error
    assert raised.value is error
    assert offline_writer == []
    assert not write_mod.os.path.exists(writer.temp_dir)


def test_completed_timeout_failure_is_not_treated_as_a_cancellation_poll():
    future = Future()
    future.set_exception(FutureTimeoutError("RPC timed out"))
    with pytest.raises(FutureTimeoutError, match="RPC timed out"):
        write_mod.upload._acknowledge(future, 0, stop_event=threading.Event())


def test_cancellation_interrupts_unacknowledged_upload(monkeypatch, tmp_path):
    awaiting_ack, stop, closed = threading.Event(), threading.Event(), threading.Event()
    finalized = []

    def send(request):
        awaiting_ack.set()
        return Future()  # never acknowledged

    stream = write_mod.Stream(
        SimpleNamespace(send=send, close=closed.set), SimpleNamespace(name="stream"),
    )
    monkeypatch.setattr(write_mod.bigquery_storage_v1, "BigQueryWriteClient", lambda: object())
    monkeypatch.setattr(write_mod, "_bq_write_create_stream", lambda *args: stream)
    monkeypatch.setattr(write_mod, "_bq_storage_close_stream", lambda *args: finalized.append(True))
    work, status = queue.Queue(), queue.Queue()
    path = tmp_path / "batch.feather"
    write_mod.fa.write_feather(TABLE, path)
    work.put(str(path))
    worker = threading.Thread(target=write_mod._stream_worker, args=(
        "parent", write_mod.pa_to_pb.generate(TABLE.schema), work, status, None, stop,
    ), daemon=True)
    worker.start()
    try:
        assert awaiting_ack.wait(timeout=5)
        stop.set()
        worker.join(timeout=2)
        assert not worker.is_alive()
        assert closed.is_set()
        assert finalized == []
        assert status.empty()
    finally:
        stop.set()
        worker.join(timeout=2)

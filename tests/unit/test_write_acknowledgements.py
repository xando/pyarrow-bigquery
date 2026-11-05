import importlib
from concurrent.futures import Future
from types import SimpleNamespace

import pyarrow as pa
import pytest
from google.api_core.exceptions import ServiceUnavailable
from google.cloud.bigquery_storage_v1 import types
from google.protobuf.timestamp_pb2 import Timestamp

from pyarrow.bigquery.write import upload

write_mod = importlib.import_module("pyarrow.bigquery.write")


def completed(response=None, error=None):
    future = Future()
    if error is not None:
        future.set_exception(error)
    else:
        future.set_result(response)
    return future


def test_upload_bounds_in_flight_requests_and_drains_tail(monkeypatch):
    sent, acknowledged = [], []
    monkeypatch.setattr(upload.pa_to_pb, "serialize", lambda *args: [[b"row"] * 2] * 11)

    class AppendStream:
        def send(self, request):
            sent.append(request.offset)
            assert len(sent) - len(acknowledged) <= upload._MAX_IN_FLIGHT_REQUESTS

            def result(timeout):
                assert timeout == upload._APPEND_TIMEOUT
                acknowledged.append(request.offset)
                return types.AppendRowsResponse(append_result={"offset": request.offset})

            return SimpleNamespace(result=result)

    upload.upload_data(SimpleNamespace(append_rows_stream=AppendStream()), object(), object(), 7)
    assert sent == acknowledged == list(range(7, 29, 2))


@pytest.mark.parametrize("response, message", [
    (types.AppendRowsResponse(), "no acknowledged offset"),
    (types.AppendRowsResponse(append_result={}), "no acknowledged offset"),
    (types.AppendRowsResponse(append_result={"offset": 9}), "expected 0"),
    (types.AppendRowsResponse(error={"code": 3, "message": "invalid rows"}), "invalid rows"),
    (types.AppendRowsResponse(append_result={"offset": 0}, row_errors=[
        {"index": 0, "code": 1, "message": "bad row"},
    ]), "row errors"),
])
def test_rejected_or_missing_acknowledgements_fail(response, message):
    with pytest.raises(RuntimeError, match=message):
        upload._acknowledge(completed(response), 0)


def test_async_append_failure_stops_sending_new_requests(monkeypatch):
    sent = []
    monkeypatch.setattr(upload.pa_to_pb, "serialize", lambda *args: [[b"row"]] * 10)

    def send(request):
        sent.append(request.offset)
        return completed(error=ServiceUnavailable("append failed"))

    stream = SimpleNamespace(append_rows_stream=SimpleNamespace(send=send))
    with pytest.raises(ServiceUnavailable, match="append failed"):
        upload.upload_data(stream, object(), object(), 0)
    assert sent == list(range(upload._MAX_IN_FLIGHT_REQUESTS))


def test_missing_future_response_times_out(monkeypatch):
    monkeypatch.setattr(upload, "_APPEND_TIMEOUT", 0)
    with pytest.raises(TimeoutError):
        upload._acknowledge(Future(), 0)


@pytest.fixture
def worker_backend(monkeypatch, tmp_path):
    events = []
    table = pa.table({"x": [1, 2]})
    path = tmp_path / "table.feather"
    write_mod.fa.write_feather(table, path)
    results, status = write_mod.queue.Queue(), write_mod.queue.Queue()
    results.put(str(path))
    results.put(None)

    class AppendStream:
        def close(self):
            events.append("close")

    class Client:
        transport = SimpleNamespace(close=lambda: events.append("client-close"))
        row_count = 2
        error = None

        def finalize_write_stream(self, **kwargs):
            events.append("finalize")
            if self.error:
                raise self.error
            return types.FinalizeWriteStreamResponse(row_count=self.row_count)

    client = Client()
    stream = write_mod.Stream(AppendStream(), SimpleNamespace(name="stream-1"))
    monkeypatch.setattr(write_mod.bigquery_storage_v1, "BigQueryWriteClient", lambda: client)
    monkeypatch.setattr(write_mod, "_bq_write_create_stream", lambda *args: stream)
    monkeypatch.setattr(upload, "upload_data", lambda *args: events.append("acknowledged"))
    return SimpleNamespace(client=client, events=events, results=results, status=status)


def run_worker(backend):
    write_mod._stream_worker("projects/p/datasets/d/tables/t", object(),
                             backend.results, backend.status)
    return backend.status.get_nowait()


def test_worker_reports_success_only_after_acknowledgement_and_finalization(worker_backend):
    assert run_worker(worker_backend) == (write_mod._QUEUE_DONE, "stream-1")
    assert worker_backend.events == ["acknowledged", "close", "finalize", "client-close"]


@pytest.mark.parametrize("failure", ["exception", "wrong_count"])
def test_finalization_failures_are_reported_as_worker_errors(worker_backend, failure):
    if failure == "exception":
        worker_backend.client.error = RuntimeError("finalize failed")
        message = "finalize failed"
    else:
        worker_backend.client.row_count = 1
        message = "returned 1 rows, expected 2"
    kind, payload = run_worker(worker_backend)
    assert kind == write_mod._QUEUE_ERROR
    assert message in payload[1]
    assert worker_backend.status.empty()


def test_upload_failure_never_finalizes_or_commits(worker_backend, monkeypatch):
    def fail(*args):
        raise RuntimeError("upload failed")

    monkeypatch.setattr(upload, "upload_data", fail)
    kind, payload = run_worker(worker_backend)
    assert kind == write_mod._QUEUE_ERROR
    assert "upload failed" in payload[1]
    assert worker_backend.events == ["close", "client-close"]


@pytest.mark.parametrize("response, message", [
    (types.BatchCommitWriteStreamsResponse(), "no commit time"),
    (types.BatchCommitWriteStreamsResponse(stream_errors=[
        {"code": 1, "error_message": "stream failed", "entity": "stream-1"},
    ]), "stream failed"),
    (types.BatchCommitWriteStreamsResponse(commit_time=Timestamp(seconds=1)), None),
])
def test_commit_checks_response_and_always_closes_client(monkeypatch, response, message):
    requests, closed = [], []

    def commit(request):
        requests.append(request)
        return response

    client = SimpleNamespace(
        batch_commit_write_streams=commit,
        transport=SimpleNamespace(close=lambda: closed.append(True)),
    )
    monkeypatch.setattr(write_mod.bigquery_storage_v1, "BigQueryWriteClient", lambda: client)
    if message:
        with pytest.raises(RuntimeError, match=message):
            write_mod._bq_storage_commit_streams("parent", ["one", "two"])
    else:
        write_mod._bq_storage_commit_streams("parent", ["one", "two"])
    assert requests[0].parent == "parent"
    assert list(requests[0].write_streams) == ["one", "two"]
    assert closed == [True]


@pytest.fixture
def coordinator(monkeypatch):
    commits = []

    def worker(parent, schema, work, status, compression=None):
        while work.get() is not None:
            pass
        status.put((write_mod._QUEUE_DONE, "stream"))

    monkeypatch.setattr(write_mod, "_stream_worker", worker)
    monkeypatch.setattr(write_mod, "_bq_storage_commit_streams",
                        lambda parent, names: commits.append((parent, list(names))))
    return commits


def test_successful_context_commits_all_streams_in_one_request(coordinator):
    with write_mod.writer(pa.schema([("x", pa.int64())]), "p.d.t",
                          table_create=False, worker_count=2):
        pass
    assert coordinator == [("projects/p/datasets/d/tables/t", ["stream", "stream"])]


def test_body_exception_prevents_commit_and_is_preserved(coordinator):
    error = ValueError("abort upload")
    with pytest.raises(ValueError) as raised:
        with write_mod.writer(pa.schema([("x", pa.int64())]), "p.d.t",
                              table_create=False, worker_count=1):
            raise error
    assert raised.value is error
    assert coordinator == []


def test_commit_failure_propagates_and_removes_temporary_files(coordinator, monkeypatch):
    def fail(*args):
        raise RuntimeError("commit failed")

    monkeypatch.setattr(write_mod, "_bq_storage_commit_streams", fail)
    with pytest.raises(RuntimeError, match="commit failed"):
        with write_mod.writer(pa.schema([("x", pa.int64())]), "p.d.t",
                              table_create=False, worker_count=1) as writer:
            writer.write_table(pa.table({"x": [1]}))
    assert not write_mod.os.path.exists(writer.temp_dir)


def test_failed_worker_prevents_commit_of_successful_peers(coordinator, monkeypatch):
    statuses = iter([
        (write_mod._QUEUE_DONE, "successful-stream"),
        (write_mod._QUEUE_ERROR, ("RuntimeError", "peer failed", "traceback")),
    ])

    def worker(parent, schema, work, status, compression=None):
        while work.get() is not None:
            pass
        status.put(next(statuses))

    monkeypatch.setattr(write_mod, "_stream_worker", worker)
    with pytest.raises(RuntimeError, match="peer failed"):
        with write_mod.writer(pa.schema([("x", pa.int64())]), "p.d.t",
                              table_create=False, worker_count=2):
            pass
    assert coordinator == []

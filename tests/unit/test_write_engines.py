import importlib
import multiprocessing
import threading
from types import SimpleNamespace

import pyarrow as pa
import pytest


@pytest.fixture
def rust_backend(monkeypatch):
    write_mod = importlib.import_module("pyarrow.bigquery.write")
    events = []
    instances = []

    class FakePyWriter:
        def __init__(self, **kwargs):
            self.options = kwargs
            self.batches = []
            self.commits = []
            self.write_error = None
            self.close_error = None
            instances.append(self)
            events.append("validate")

        def write_batch(self, batch):
            if self.write_error:
                raise self.write_error
            self.batches.append(batch)

        def close(self, *, commit=True):
            self.commits.append(commit)
            if self.close_error:
                raise self.close_error

    def create_table(**kwargs):
        events.append(("create", kwargs))

    monkeypatch.setattr(write_mod._rust, "PyWriter", FakePyWriter, raising=False)
    monkeypatch.setattr(write_mod, "_bq_create_table", create_table)
    return SimpleNamespace(module=write_mod, events=events, instances=instances)


def test_rust_writer_passes_arrow_batches_without_python_workers(rust_backend, monkeypatch):
    write_mod = rust_backend.module

    def unexpected_python_path(*args, **kwargs):
        pytest.fail("Rust writes should not use Python workers or temporary files")

    monkeypatch.setattr(write_mod.tempfile, "mkdtemp", unexpected_python_path)
    monkeypatch.setattr(write_mod.pa_to_pb, "generate", unexpected_python_path)
    monkeypatch.setattr(write_mod.fa, "write_feather", unexpected_python_path)
    monkeypatch.setattr(write_mod, "_stream_worker", unexpected_python_path)

    table = pa.table({"x": pa.chunked_array([[1, 2], [3]])})
    batch = pa.record_batch([[4]], names=["x"])
    w = write_mod.writer(
        table.schema,
        "destination.dataset.table",
        project="billing",
        table_expire=3600,
        table_overwrite=True,
        worker_count=3,
        engine="rust",
    )
    with w as entered:
        assert entered is w
        w.write_table(table)
        w.write_batch(batch)

    native = rust_backend.instances[0]
    assert native.options == {
        "parent": "projects/destination/datasets/dataset/tables/table",
        "schema": table.schema,
        "worker_count": 3,
    }
    assert rust_backend.events == [
        "validate",
        (
            "create",
            {
                "project": "billing",
                "location": "destination.dataset.table",
                "schema": table.schema,
                "expire": 3600,
                "overwrite": True,
            },
        ),
    ]
    assert [b.num_rows for b in native.batches] == [2, 1, 1]
    assert native.batches[-1] is batch
    assert pa.Table.from_batches(native.batches).to_pydict() == {"x": [1, 2, 3, 4]}
    assert native.commits == [True]
    assert not hasattr(w, "temp_dir")
    assert not hasattr(w, "workers")


@pytest.mark.parametrize("engine", [None, "invalid", "Rust"])
def test_writer_rejects_unknown_engine_before_side_effects(rust_backend, engine):
    with pytest.raises(ValueError, match="Unsupported engine"):
        rust_backend.module.write_table(
            pa.table({"x": [1]}), "project.dataset.table", engine=engine
        )
    assert rust_backend.events == []


@pytest.mark.parametrize("worker_count", [0, -1, 1.5, None, "2"])
def test_rust_writer_requires_positive_integer_worker_count(rust_backend, worker_count):
    with pytest.raises(ValueError, match="worker_count must be a positive integer"):
        rust_backend.module.writer(
            pa.schema([("x", pa.int64())]),
            "project.dataset.table",
            worker_count=worker_count,
            engine="rust",
        )
    assert rust_backend.events == []


def test_rust_writer_warns_for_ignored_process_workers(rust_backend):
    with pytest.warns(UserWarning, match="worker_type.*ignored"):
        w = rust_backend.module.writer(
            pa.schema([("x", pa.int64())]),
            "project.dataset.table",
            worker_type=multiprocessing.Process,
            engine="rust",
        )
    assert w.engine == "rust"


def test_rust_writer_still_rejects_invalid_worker_types(rust_backend):
    with pytest.raises(ValueError, match="Unsupported worker type"):
        rust_backend.module.writer(
            pa.schema([("x", pa.int64())]),
            "project.dataset.table",
            worker_type=object,
            engine="rust",
        )


def test_rust_writer_append_skips_table_creation(rust_backend):
    table = pa.table({"x": [1]})
    with rust_backend.module.writer(
        table.schema, "project.dataset.table", table_create=False, engine="rust"
    ) as w:
        assert w.project == "project"
        w.write_table(table)
    assert rust_backend.events == ["validate"]
    assert rust_backend.instances[0].commits == [True]


def test_rust_validation_precedes_overwrite(rust_backend, monkeypatch):
    def invalid_schema(**kwargs):
        raise ValueError("unsupported Arrow type")

    monkeypatch.setattr(rust_backend.module._rust, "PyWriter", invalid_schema)
    with pytest.raises(ValueError, match="unsupported Arrow type"):
        with rust_backend.module.writer(
            pa.schema([("x", pa.list_(pa.list_(pa.int64())))]),
            "project.dataset.table",
            table_overwrite=True,
            engine="rust",
        ):
            pytest.fail("Invalid schema must prevent context entry")
    assert rust_backend.events == []


def test_table_schema_validation_precedes_client_and_overwrite(monkeypatch):
    write_mod = importlib.import_module("pyarrow.bigquery.write")

    def unexpected_client(**kwargs):
        pytest.fail("Invalid schema must not create a client or delete a table")

    monkeypatch.setattr(write_mod.bigquery, "Client", unexpected_client)
    with pytest.raises(TypeError, match="Nested lists are not supported"):
        write_mod._bq_create_table(
            project="project",
            location="project.dataset.table",
            schema=pa.schema([("x", pa.list_(pa.list_(pa.int64())))]),
            expire=None,
            overwrite=True,
        )


@pytest.mark.parametrize("cleanup_fails", [False, True])
def test_rust_setup_failure_closes_without_commit(rust_backend, monkeypatch, cleanup_fails):
    original_error = RuntimeError("create failed")

    def fail_create(**kwargs):
        if cleanup_fails:
            rust_backend.instances[0].close_error = RuntimeError("cleanup failed")
        raise original_error

    monkeypatch.setattr(rust_backend.module, "_bq_create_table", fail_create)
    w = rust_backend.module.writer(
        pa.schema([("x", pa.int64())]), "project.dataset.table", engine="rust"
    )
    with pytest.raises(RuntimeError, match="create failed") as exc:
        w.__enter__()
    assert exc.value is original_error
    assert rust_backend.instances[0].commits == [False]
    with pytest.raises(RuntimeError, match="writer is not open"):
        w.write_table(pa.table({"x": [1]}))


@pytest.mark.parametrize("cleanup_fails", [False, True])
def test_rust_context_preserves_body_exception_without_commit(rust_backend, cleanup_fails):
    original_error = ValueError("body failed")
    with pytest.raises(ValueError, match="body failed") as exc:
        with rust_backend.module.writer(
            pa.schema([("x", pa.int64())]), "project.dataset.table", engine="rust"
        ):
            if cleanup_fails:
                rust_backend.instances[0].close_error = RuntimeError("cleanup failed")
            raise original_error
    assert exc.value is original_error
    assert rust_backend.instances[0].commits == [False]


def test_rust_write_failure_prevents_commit(rust_backend):
    with pytest.raises(RuntimeError, match="upload failed"):
        with rust_backend.module.writer(
            pa.schema([("x", pa.int64())]), "project.dataset.table", engine="rust"
        ) as w:
            rust_backend.instances[0].write_error = RuntimeError("upload failed")
            w.write_batch(pa.record_batch([[1]], names=["x"]))
    assert rust_backend.instances[0].commits == [False]


def test_rust_commit_failure_propagates_and_closes_context(rust_backend):
    with pytest.raises(RuntimeError, match="commit failed"):
        with rust_backend.module.writer(
            pa.schema([("x", pa.int64())]), "project.dataset.table", engine="rust"
        ) as w:
            rust_backend.instances[0].close_error = RuntimeError("commit failed")
    assert rust_backend.instances[0].commits == [True]
    with pytest.raises(RuntimeError, match="writer is not open"):
        w.write_batch(pa.record_batch([[1]], names=["x"]))


@pytest.mark.parametrize("method", ["write_table", "write_batch"])
def test_rust_write_requires_open_context(rust_backend, method):
    table = pa.table({"x": [1]})
    data = table if method == "write_table" else table.to_batches()[0]
    w = rust_backend.module.writer(table.schema, "project.dataset.table", engine="rust")
    with pytest.raises(RuntimeError, match="writer is not open"):
        getattr(w, method)(data)
    with w:
        getattr(w, method)(data)
    with pytest.raises(RuntimeError, match="writer is not open"):
        getattr(w, method)(data)


def test_rust_write_table_wrapper_preserves_chunking_and_options(rust_backend):
    table = pa.table({"x": [1, 2, 3, 4, 5]})
    result = rust_backend.module.write_table(
        table,
        "project.dataset.table",
        engine="rust",
        worker_count=2,
        table_create=False,
        batch_size=2,
    )
    assert result is None
    native = rust_backend.instances[0]
    assert native.options["worker_count"] == 2
    assert [b.num_rows for b in native.batches] == [2, 2, 1]
    assert native.commits == [True]
    assert rust_backend.events == ["validate"]


@pytest.mark.parametrize("engine", ["python", "rust"])
def test_write_table_still_rejects_empty_input(rust_backend, engine):
    with pytest.raises(AssertionError, match="Table is empty"):
        rust_backend.module.write_table(
            pa.schema([("x", pa.int64())]).empty_table(),
            "project.dataset.table",
            engine=engine,
        )
    assert rust_backend.events == []


def test_default_writer_preserves_python_workers(rust_backend, monkeypatch):
    write_mod = rust_backend.module
    received = []

    def worker(parent, schema_protobuf, queue_results, queue_worker_status):
        received.append((parent, schema_protobuf))
        assert queue_results.get(timeout=1) is None
        queue_worker_status.put((write_mod._QUEUE_DONE, None))

    descriptor = object()
    monkeypatch.setattr(write_mod, "_stream_worker", worker)
    monkeypatch.setattr(write_mod.pa_to_pb, "generate", lambda schema: descriptor)
    with write_mod.writer(
        pa.schema([("x", pa.int64())]),
        "project.dataset.table",
        worker_count=1,
        table_create=False,
    ) as w:
        assert w.engine == "python"
        assert w.worker_type is threading.Thread
        assert len(w.workers) == 1
        assert w.queue_results.maxsize == 4
    assert received == [("projects/project/datasets/dataset/tables/table", descriptor)]
    assert rust_backend.instances == []

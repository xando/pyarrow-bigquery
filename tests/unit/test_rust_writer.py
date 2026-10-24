"""Exercise the native writer's offline validation and Arrow C interface."""

import pyarrow as pa
import pytest

import pyarrow.bigquery._rust as _rust

PARENT = "projects/offline-project/datasets/dataset/tables/table"


@pytest.fixture(autouse=True)
def no_cloud_credentials(monkeypatch, tmp_path):
    # An accidental auth attempt fails locally instead of using the developer's
    # credentials or contacting a metadata server. These operations need no auth.
    monkeypatch.setenv(
        "GOOGLE_APPLICATION_CREDENTIALS", str(tmp_path / "missing-credentials.json")
    )


@pytest.mark.parametrize("commit", [True, False])
def test_native_constructor_and_idempotent_close_are_offline(commit):
    writer = _rust.PyWriter(
        parent=PARENT,
        schema=pa.schema([("x", pa.int64())]),
        worker_count=2,
    )
    assert writer.close(commit=commit) is None
    assert writer.close(commit=commit) is None
    assert writer.close(commit=not commit) is None


def test_native_empty_batch_uses_arrow_interface_without_opening_streams():
    schema = pa.schema(
        [
            ("x", pa.int64()),
            ("timestamp", pa.timestamp("ns")),
            ("items", pa.list_(pa.struct([("value", pa.string())]))),
        ]
    )
    writer = _rust.PyWriter(PARENT, schema)
    batch = pa.RecordBatch.from_arrays(
        [pa.array([], type=field.type) for field in schema], schema=schema
    )
    assert writer.write_batch(batch) is None
    assert writer.write_batch(batch) is None
    assert writer.close() is None


@pytest.mark.parametrize(
    "parent",
    [
        "project.dataset.table",
        "projects/project/datasets/dataset/tables",
        "projects//datasets/dataset/tables/table",
        "projects/project/datasets/dataset/tables/table/extra",
    ],
)
def test_native_rejects_invalid_parent(parent):
    with pytest.raises(ValueError, match="parent must be"):
        _rust.PyWriter(parent, pa.schema([("x", pa.int64())]))


@pytest.mark.parametrize(
    "schema",
    [
        pa.schema([]),
        pa.schema([("x", pa.list_(pa.list_(pa.int64())))]),
        pa.schema([("x", pa.map_(pa.string(), pa.int64()))]),
    ],
)
def test_native_rejects_unsupported_schema(schema):
    with pytest.raises(ValueError, match="unsupported BigQuery Arrow schema"):
        _rust.PyWriter(PARENT, schema)


@pytest.mark.parametrize(
    ("worker_count", "error_type"),
    [(0, ValueError), (-1, OverflowError), (1.5, TypeError)],
)
def test_native_rejects_invalid_worker_count(worker_count, error_type):
    with pytest.raises(error_type):
        _rust.PyWriter(
            PARENT, pa.schema([("x", pa.int64())]), worker_count=worker_count
        )


@pytest.mark.parametrize("failure", ["schema", "timestamp_precision"])
def test_native_validation_failure_poison_prevents_later_writes_and_commit(failure):
    if failure == "schema":
        schema = pa.schema([("x", pa.int64())])
        bad_batch = pa.record_batch([[1]], names=["other"])
        message = "record batch schema does not match the writer schema"
    else:
        schema = pa.schema([("x", pa.timestamp("ns"))])
        bad_batch = pa.RecordBatch.from_arrays(
            [pa.array([1], type=pa.timestamp("ns"))], schema=schema
        )
        message = "values must be whole microseconds"

    writer = _rust.PyWriter(PARENT, schema, worker_count=1)
    with pytest.raises(RuntimeError, match=message):
        writer.write_batch(bad_batch)

    # Even a valid empty batch cannot clear the first error. All of these
    # operations fail before authentication or a stream can be started.
    empty_batch = pa.RecordBatch.from_arrays(
        [pa.array([], type=field.type) for field in schema], schema=schema
    )
    with pytest.raises(RuntimeError, match=message):
        writer.write_batch(empty_batch)
    for commit in [True, False, True]:
        with pytest.raises(RuntimeError, match=message):
            writer.close(commit=commit)


def test_native_writer_rejects_batches_after_close():
    schema = pa.schema([("x", pa.int64())])
    writer = _rust.PyWriter(PARENT, schema)
    writer.close()
    empty_batch = pa.record_batch([pa.array([], type=pa.int64())], schema=schema)
    with pytest.raises(RuntimeError, match="writer is closed"):
        writer.write_batch(empty_batch)

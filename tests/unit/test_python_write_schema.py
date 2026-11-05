import importlib
import queue

import pyarrow as pa
import pytest

write_mod = importlib.import_module("pyarrow.bigquery.write")
SCHEMA = pa.schema([
    pa.field("id", pa.int64()),
    pa.field("record", pa.struct([("name", pa.string())])),
])


@pytest.fixture
def python_writer(tmp_path):
    # Exercise the real enqueue/Feather path without clients or workers.
    writer = write_mod.writer(SCHEMA, "project.dataset.table", table_create=False)
    writer.temp_dir = str(tmp_path)
    writer.queue_results = queue.Queue()
    return writer


@pytest.mark.parametrize("method", ["write_table", "write_batch"])
@pytest.mark.parametrize("empty", [False, True])
@pytest.mark.parametrize("schema", [
    pa.schema([SCHEMA.field("id")]),  # missing field
    SCHEMA.append(pa.field("extra", pa.string())),
    pa.schema(list(reversed(SCHEMA))),
    SCHEMA.set(0, pa.field("wrong_name", pa.int64())),
    SCHEMA.set(0, pa.field("id", pa.string())),
    SCHEMA.set(0, pa.field("id", pa.int64(), nullable=False)),
    SCHEMA.set(1, pa.field("record", pa.struct([("renamed", pa.string())]))),
    SCHEMA.set(1, pa.field("record", pa.struct([("name", pa.int64())]))),
    SCHEMA.set(1, pa.field("record", pa.list_(pa.string()))),
])
def test_schema_mismatch_fails_before_file_creation_or_enqueue(
    python_writer, monkeypatch, tmp_path, method, empty, schema,
):
    def unexpected_file(*args, **kwargs):
        pytest.fail("schema mismatch must be detected before creating a file")

    monkeypatch.setattr(write_mod.tempfile, "mkstemp", unexpected_file)
    arrays = [pa.array([] if empty else [None], type=field.type) for field in schema]
    batch = pa.RecordBatch.from_arrays(arrays, schema=schema)
    data = pa.Table.from_batches([batch]) if method == "write_table" else batch
    with pytest.raises(ValueError, match="Input schema does not match the writer schema"):
        getattr(python_writer, method)(data)
    assert python_writer.queue_results.empty()
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("method", ["write_table", "write_batch"])
@pytest.mark.parametrize("metadata", [None, {b"source": b"etl"}])
def test_matching_schema_is_queued_without_changing_values(python_writer, method, metadata):
    schema = SCHEMA.with_metadata(metadata)
    if metadata:
        schema = schema.set(0, schema.field(0).with_metadata(metadata))
    table = pa.Table.from_pylist(
        [{"id": 42, "record": {"name": "alice"}}, {"id": None, "record": None}],
        schema=schema,
    )
    data = table if method == "write_table" else table.to_batches()[0]
    getattr(python_writer, method)(data)
    path = python_writer.queue_results.get_nowait()
    assert write_mod.fa.read_table(path).equals(table, check_metadata=True)
    assert python_writer.queue_results.empty()

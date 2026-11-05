"""Invalid sizes must fail locally, before queries or destructive table operations."""

import importlib

import pyarrow as pa
import pytest

read_mod = importlib.import_module("pyarrow.bigquery.read")
write_mod = importlib.import_module("pyarrow.bigquery.write")
INVALID_SIZES = [0, -1, 1.5, None, "2", True, False]


@pytest.fixture(autouse=True)
def no_side_effects(monkeypatch):
    def unexpected(*args, **kwargs):
        pytest.fail("invalid input must be rejected before clients, workers or table operations")

    monkeypatch.setattr(read_mod.bigquery, "Client", unexpected)
    monkeypatch.setattr(read_mod.bigquery_storage, "BigQueryReadClient", unexpected)
    monkeypatch.setattr(read_mod, "_bq_table_exists", unexpected)
    monkeypatch.setattr(write_mod, "_bq_create_table", unexpected)
    monkeypatch.setattr(write_mod.tempfile, "mkdtemp", unexpected)
    monkeypatch.setattr(write_mod._rust, "PyWriter", unexpected)
    monkeypatch.setattr(read_mod._rust, "PyReader", unexpected)


@pytest.mark.parametrize("engine", ["python", "rust"])
@pytest.mark.parametrize("api", ["reader", "read_table", "reader_query", "read_query"])
@pytest.mark.parametrize("parameter", ["worker_count", "batch_size"])
@pytest.mark.parametrize("value", INVALID_SIZES)
def test_read_sizes_are_validated_before_side_effects(engine, api, parameter, value):
    args = ("project", "SELECT 1") if "query" in api else ("project.dataset.table",)
    kwargs = {parameter: value, "engine": engine}
    if "query" in api:
        kwargs["large_results"] = True
    with pytest.raises(ValueError, match=f"{parameter} must be a positive integer"):
        getattr(read_mod, api)(*args, **kwargs)


@pytest.mark.parametrize("engine", ["python", "rust"])
@pytest.mark.parametrize("api", ["writer", "write_table"])
@pytest.mark.parametrize("value", INVALID_SIZES)
def test_write_worker_count_is_validated_before_side_effects(engine, api, value):
    table = pa.table({"x": [1]})
    data = table.schema if api == "writer" else table
    with pytest.raises(ValueError, match="worker_count must be a positive integer"):
        getattr(write_mod, api)(
            data, "project.dataset.table", engine=engine, worker_count=value,
            table_overwrite=True,
        )


@pytest.mark.parametrize("engine", ["python", "rust"])
@pytest.mark.parametrize("value", INVALID_SIZES)
def test_write_batch_size_is_validated_before_overwrite(engine, value):
    with pytest.raises(ValueError, match="batch_size must be a positive integer"):
        write_mod.write_table(
            pa.table({"x": [1]}), "project.dataset.table", engine=engine,
            batch_size=value, table_overwrite=True,
        )


@pytest.mark.parametrize("engine", ["python", "rust"])
@pytest.mark.parametrize("compression", ["snappy", [], False])
def test_invalid_read_compression_raises_explicit_value_error(engine, compression):
    with pytest.raises(ValueError, match="Compression .* not supported"):
        read_mod.reader("project.dataset.table", engine=engine, compression=compression)


@pytest.mark.parametrize("engine", ["python", "rust"])
def test_positive_sizes_are_accepted(engine):
    r = read_mod.reader("project.dataset.table", worker_count=1, batch_size=1, engine=engine)
    w = write_mod.writer(pa.schema([("x", pa.int64())]), "project.dataset.table",
                         worker_count=1, engine=engine)
    assert r.worker_count == r.batch_size == w.worker_count == 1

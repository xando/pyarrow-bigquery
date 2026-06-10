import importlib

import pyarrow as pa

SCHEMA = pa.schema([("x", pa.int64()), ("y", pa.string())])


def test_python_reader_yields_nothing_when_no_streams(monkeypatch):
    """engine="python": an empty result set (no streams) iterates as empty."""
    read_mod = importlib.import_module("pyarrow.bigquery.read")

    monkeypatch.setattr(read_mod, "_bq_table_exists", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        read_mod,
        "_bq_read_create_streams",
        lambda **kwargs: ([], SCHEMA),
    )

    def fail_stream_worker(*args, **kwargs):
        raise AssertionError("no worker should be started for an empty result set")

    monkeypatch.setattr(read_mod, "_stream_worker", fail_stream_worker)

    with read_mod.reader("project.dataset.table", worker_count=4, engine="python") as r:
        assert list(r) == []
        assert r.schema == SCHEMA


def test_read_table_empty_preserves_schema_python(monkeypatch):
    """engine="python": read_table returns a zero-row table with the schema."""
    read_mod = importlib.import_module("pyarrow.bigquery.read")

    monkeypatch.setattr(read_mod, "_bq_table_exists", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        read_mod,
        "_bq_read_create_streams",
        lambda **kwargs: ([], SCHEMA),
    )

    table = read_mod.read_table(
        "project.dataset.table",
        row_restrictions="x = -1",
        engine="python",
    )

    assert table.num_rows == 0
    assert table.schema == SCHEMA


def test_read_table_empty_preserves_schema_rust(monkeypatch):
    """engine="rust": read_table returns a zero-row table with the schema."""
    read_mod = importlib.import_module("pyarrow.bigquery.read")

    class FakePyReader:
        def __init__(self, *args, **kwargs):
            self._schema_ipc = SCHEMA.serialize().to_pybytes()

        def schema_ipc(self):
            return self._schema_ipc

        def __iter__(self):
            return self

        def __next__(self):
            raise StopIteration

    monkeypatch.setattr(read_mod, "_bq_table_exists", lambda *args, **kwargs: None)
    monkeypatch.setattr(read_mod._rust, "PyReader", FakePyReader)

    table = read_mod.read_table(
        "project.dataset.table",
        row_restrictions="x = -1",
        engine="rust",
    )

    assert table.num_rows == 0
    assert table.schema == SCHEMA

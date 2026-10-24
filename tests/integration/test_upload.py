import os
import uuid

import pyarrow as pa
import pytest

import pyarrow.bigquery as bq

PROJECT = os.environ["GCP_PROJECT"]


@pytest.fixture(scope="function")
def LOCATION():
    return f"{PROJECT}.test.{uuid.uuid4()}"


@pytest.fixture(autouse=True, scope="function")
def _remove_table(LOCATION):
    from google.cloud import bigquery

    bigquery.Client(project=PROJECT).delete_table(LOCATION, not_found_ok=True)
    yield
    bigquery.Client(project=PROJECT).delete_table(LOCATION, not_found_ok=True)


@pytest.mark.parametrize("engine", ["python", "rust"])
def test_simple(LOCATION, engine):
    table = pa.Table.from_arrays([[1, 2, 3, 4]], names=["test"])

    bq.write_table(table, LOCATION, table_create=True, engine=engine)

    table_back = bq.read_table(LOCATION)

    assert table_back.schema == table.schema
    assert table_back.sort_by("test").equals(table.sort_by("test"))


def test_reader_query(LOCATION):
    table = pa.Table.from_arrays([[1, 2, 3, 4]], names=["test"])

    bq.write_table(table, LOCATION, table_create=True)

    query = f"SELECT * FROM `{LOCATION}`"

    with bq.reader_query(project=PROJECT, query=query) as r:
        table_back1 = pa.concat_tables(r)

    table_back2 = bq.read_query(project=PROJECT, query=query)

    assert table_back1.schema == table_back2.schema == table.schema

    assert table_back1.sort_by("test").equals(table.sort_by("test"))
    assert table_back2.sort_by("test").equals(table.sort_by("test"))


@pytest.mark.parametrize("engine", ["python", "rust"])
def test_context(LOCATION, engine):
    table = pa.Table.from_arrays([[1, 2, 3, 4]], names=["test"])

    with bq.writer(table.schema, LOCATION, table_create=True, engine=engine) as w:
        w.write_table(table)

    with bq.reader(LOCATION) as r:
        table_back = pa.concat_tables(r)

    assert table_back.schema == table.schema
    assert table_back.sort_by("test").equals(table.sort_by("test"))


@pytest.mark.parametrize("engine", ["python", "rust"])
def test_read_table_empty_preserves_schema(LOCATION, engine):
    table = pa.Table.from_arrays([[1, 2, 3, 4]], names=["test"])

    bq.write_table(table, LOCATION, table_create=True)

    table_back = bq.read_table(LOCATION, row_restrictions="test = -1", engine=engine)

    assert table_back.num_rows == 0
    assert table_back.schema == table.schema


@pytest.mark.parametrize("engine", ["python", "rust"])
def test_read_query_empty_preserves_schema(LOCATION, engine):
    table = pa.Table.from_arrays([[1, 2, 3, 4]], names=["test"])

    bq.write_table(table, LOCATION, table_create=True)

    query = f"SELECT * FROM `{LOCATION}` WHERE test = -1"

    table_back = bq.read_query(project=PROJECT, query=query, engine=engine)

    assert table_back.num_rows == 0
    assert table_back.schema == table.schema


@pytest.mark.parametrize("engine", ["python", "rust"])
def test_complex(LOCATION, engine):
    _1d_int = pa.array([1, 2])
    _2d_int = pa.array([[1, 2, 3], [4, 5, 6]])
    _3d_int = pa.array(
        [
            [{"values": [1, 2, 3]}, {"values": [4, 5, 6]}],
            [{"values": [4, 5, 6]}, {"values": [4, 5, 6]}],
        ]
    )

    struct = pa.array(
        [{"key_1": "value_1", "key_2": "value_2"}, {"key_1": "value_1", "key_2": "value_2"}]
    )
    repeat_struct = pa.array(
        [
            [{"key_1": "value_1", "key_2": "value_2"}, {"key_1": "value_1", "key_2": "value_2"}],
            [{"key_1": "value_1", "key_2": "value_2"}, {"key_1": "value_1", "key_2": "value_2"}],
        ]
    )

    _1d_float = pa.array([1.0, 2.0])
    _2d_float = pa.array([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]])
    _3d_float = pa.array(
        [
            [{"values": [1.0, 2.0, 3.0]}, {"values": [4.0, 5.0, 6.0]}],
            [{"values": [4.0, 5.0, 6.0]}, {"values": [4.0, 5.0, 6.0]}],
        ]
    )

    table = pa.Table.from_arrays(
        [
            _1d_int,
            _2d_int,
            _3d_int,
            struct,
            repeat_struct,
            _1d_float,
            _2d_float,
            _3d_float,
        ],
        names=[
            "_1d_int",
            "_2d_int",
            "_3d_int",
            "struct",
            "repeat_struct",
            "_1d_float",
            "_2d_float",
            "_3d_float",
        ],
    )

    bq.write_table(
        table,
        LOCATION,
        table_create=True,
        engine=engine,
    )

    table_back = bq.read_table(LOCATION)

    assert table.sort_by("_1d_int").to_pylist() == table_back.sort_by("_1d_int").to_pylist()


def test_rust_write_batch_appends_to_existing_table(LOCATION):
    initial = pa.table({"value": [1, 2]})
    bq.write_table(initial, LOCATION, engine="rust", worker_count=2)

    batch = pa.record_batch([pa.array([3, 4])], schema=initial.schema)
    with bq.writer(initial.schema, LOCATION, table_create=False, engine="rust") as w:
        w.write_batch(batch)

    assert bq.read_table(LOCATION).sort_by("value").to_pydict() == {"value": [1, 2, 3, 4]}


def test_rust_context_exception_does_not_commit_rows(LOCATION):
    table = pa.table({"value": [1, 2]})
    with pytest.raises(ValueError, match="abort upload"):
        with bq.writer(table.schema, LOCATION, engine="rust") as w:
            w.write_table(table)
            raise ValueError("abort upload")

    assert bq.read_table(LOCATION).num_rows == 0


def test_rust_normalizes_timestamps_and_null_lists(LOCATION):
    table = pa.table({
        "id": [1, 2],
        "at": pa.array([1_700_000_000_123, None], type=pa.timestamp("ms")),
        "items": pa.array([None, [1, 2]], type=pa.list_(pa.int64())),
    })
    bq.write_table(table, LOCATION, engine="rust", worker_count=2)

    result = bq.read_table(LOCATION).sort_by("id")
    assert result["at"].cast(pa.int64()).to_pylist() == [1_700_000_000_123_000, None]
    assert result["items"].to_pylist() == [[], [1, 2]]

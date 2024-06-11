import pytest
import uuid
import os

import pyarrow as pa
import pyarrow.bigquery as bq


PROJECT = os.environ['GCP_PROJECT']
LOCATION = f"{PROJECT}.test.{uuid.uuid4()}"


@pytest.fixture(autouse=True)
def _remove_table():
    from google.cloud import bigquery

    bigquery.Client(project="xando-1-main").delete_table(LOCATION, not_found_ok=True)
    yield
    bigquery.Client(project="xando-1-main").delete_table(LOCATION, not_found_ok=True)


def test_simple():
    table = pa.Table.from_arrays([[1, 2, 3, 4]], names=["test"])

    bq.write_table(table, LOCATION, table_create=True)

    table_back = bq.read_table(LOCATION)

    assert table_back.schema == table.schema
    assert table_back.sort_by("test").equals(table.sort_by("test"))


def test_reader_query():
    table = pa.Table.from_arrays([[1, 2, 3, 4]], names=["test"])

    bq.write_table(table, LOCATION, table_create=True)

    query = f'SELECT * FROM `{LOCATION}`'
    table_back1 = pa.concat_tables([t for t in bq.reader_query(project=PROJECT, query=query)])

    table_back2 = bq.read_query(project=PROJECT, query=query)

    assert table_back1.schema == table_back2.schema == table.schema

    assert table_back1.sort_by("test").equals(table.sort_by("test"))
    assert table_back2.sort_by("test").equals(table.sort_by("test"))


def test_context():
    table = pa.Table.from_arrays([[1, 2, 3, 4]], names=["test"])

    with bq.writer(table.schema, LOCATION, table_create=True) as w:
        w.write_table(table)

    table_back = pa.concat_tables([t for t in bq.reader(LOCATION)])

    assert table_back.schema == table.schema
    assert table_back.sort_by("test").equals(table.sort_by("test"))


def test_complex():
    _1d_int = pa.array([1, 2])
    _2d_int = pa.array([[1, 2, 3], [4, 5, 6]])
    _3d_int = pa.array(
        [
            [{"values": [1, 2, 3]}, {"values": [4, 5, 6]}],
            [{"values": [4, 5, 6]}, {"values": [4, 5, 6]}],
        ]
    )

    struct = pa.array([{"key_1": "value_1", "key_2": "value_2"}, {"key_1": "value_1", "key_2": "value_2"}])
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
    )

    table_back = bq.read_table(LOCATION)

    assert table.sort_by("_1d_int").to_pylist() == table_back.sort_by("_1d_int").to_pylist()

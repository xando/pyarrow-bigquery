import pytest
import pyarrow as pa
from pyarrow.bigquery.write.pa_to_pb import ammend_schema


def test_1d():
    schema = pa.schema(
        [
            ("_1d_date32", pa.date32()),
            ("_1d_date64", pa.date64()),
            ("_1d_decimal", pa.decimal128(5)),
            ("_1d_time32", pa.time32("s")),
            ("_1d_time64", pa.time64("ns")),
            ("_1d_timestamp", pa.timestamp("s")),
        ]
    )

    _schema = ammend_schema(schema)

    assert len(schema) == len(_schema)
    assert all(t.type == pa.string() for t in _schema)


def test_2d():
    schema = pa.schema(
        [
            ("_2d_date32", pa.list_(pa.date32())),
            ("_2d_date64", pa.list_(pa.date64())),
            ("_2d_decimal", pa.list_(pa.decimal128(5))),
            ("_2d_time32", pa.list_(pa.time32("s"))),
            ("_2d_time64", pa.list_(pa.time64("ns"))),
            ("_2d_timestamp", pa.list_(pa.timestamp("s"))),
        ]
    )

    _schema = ammend_schema(schema)

    assert len(schema) == len(_schema)
    assert all(f.type.value_type == pa.string() for f in _schema)


def test_3d():
    schema = pa.schema(
        [
            ("date32", pa.list_(pa.struct([("key", pa.date32())]))),
            ("date64", pa.list_(pa.struct([("key", pa.date64())]))),
            ("decimal", pa.list_(pa.struct([("key", pa.decimal128(5))]))),
            ("time32", pa.list_(pa.struct([("key", pa.time32("s"))]))),
            ("time64", pa.list_(pa.struct([("key", pa.time64("us"))]))),
            ("timestamp", pa.list_(pa.struct([("key", pa.timestamp("s"))]))),
        ]
    )

    _schema = ammend_schema(schema)

    assert len(schema) == len(_schema)

    assert all(f.type.value_type["key"].type == pa.string() for f in _schema)

import pyarrow as pa
from pyarrow.bigquery.write.pa_to_bq import emit


def test_1d():
    schema = pa.schema([("_1d_int", pa.int64())])

    result = emit(schema)

    assert len(result) == 1
    assert result[0].name == "_1d_int"
    assert result[0].mode == "NULLABLE"
    assert result[0].field_type == "INTEGER"


def test_1d_nullable_false():
    schema = pa.schema([pa.field("_1d_int", pa.int64(), nullable=False)])

    result = emit(schema)

    assert len(result) == 1
    assert result[0].name == "_1d_int"
    assert result[0].mode == "REQUIRED"
    assert result[0].field_type == "INTEGER"


def test_2d():
    schema = pa.schema([("_2d_int", pa.list_(pa.int64()))])

    result = emit(schema)

    assert len(result) == 1
    assert result[0].name == "_2d_int"
    assert result[0].mode == "REPEATED"
    assert result[0].field_type == "INTEGER"


def test_struct():
    schema = pa.schema([("struct", pa.struct([("key_1", pa.string()), ("key_2", pa.string())]))])

    result = emit(schema)

    assert len(result) == 1
    assert result[0].name == "struct"
    assert result[0].mode == "NULLABLE"
    assert result[0].field_type == "RECORD"
    assert len(result[0].fields) == 2

    assert result[0].fields[0].name == "key_1"
    assert result[0].fields[0].mode == "NULLABLE"
    assert result[0].fields[0].field_type == "STRING"

    assert result[0].fields[1].name == "key_2"
    assert result[0].fields[1].mode == "NULLABLE"
    assert result[0].fields[1].field_type == "STRING"


def test_repeated_struct():
    schema = pa.schema(
        [("repeated_struct", pa.list_(pa.struct([("key_1", pa.string()), ("key_2", pa.string())])))]
    )

    result = emit(schema)

    assert len(result) == 1
    assert result[0].name == "repeated_struct"
    assert result[0].mode == "REPEATED"
    assert result[0].field_type == "RECORD"
    assert len(result[0].fields) == 2

    assert result[0].fields[0].name == "key_1"
    assert result[0].fields[0].mode == "NULLABLE"
    assert result[0].fields[0].field_type == "STRING"

    assert result[0].fields[1].name == "key_2"
    assert result[0].fields[1].mode == "NULLABLE"
    assert result[0].fields[1].field_type == "STRING"

import pyarrow as pa
from google.protobuf import message_factory
from google.protobuf.json_format import MessageToDict

from pyarrow.bigquery.write.pa_to_pb import generate, serialize


def test_serialize_nullable_struct_with_null_values():
    schema = pa.schema(
        [
            ("id", pa.int64()),
            (
                "meta",
                pa.struct(
                    [
                        ("name", pa.string()),
                        ("score", pa.int64()),
                    ]
                ),
            ),
        ]
    )
    table = pa.Table.from_pylist(
        [
            {"id": 1, "meta": None},
            {"id": 2, "meta": {"name": "alice", "score": 10}},
            {"id": 3, "meta": {"name": None, "score": 11}},
        ],
        schema=schema,
    )

    descriptor = generate(schema)
    message_cls = message_factory.GetMessageClass(descriptor)

    batches = list(serialize(table, descriptor))
    rows = [blob for batch in batches for blob in batch]

    assert len(rows) == 3

    # Use protobuf reflection for dynamically generated fields. ProtoJSON encodes
    # int64 values as strings and omits unset optional fields.
    assert [MessageToDict(message_cls.FromString(row)) for row in rows] == [
        {"id": "1"},
        {"id": "2", "meta": {"name": "alice", "score": "10"}},
        {"id": "3", "meta": {"score": "11"}},
    ]

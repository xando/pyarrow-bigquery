import pyarrow as pa

from google.protobuf import message_factory

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

    message_1 = message_cls.FromString(rows[0])
    message_2 = message_cls.FromString(rows[1])
    message_3 = message_cls.FromString(rows[2])

    assert message_1.id == 1
    assert not message_1.HasField("meta")

    assert message_2.id == 2
    assert message_2.meta.name == "alice"
    assert message_2.meta.score == 10

    assert message_3.id == 3
    assert message_3.HasField("meta")
    assert not message_3.meta.HasField("name")
    assert message_3.meta.score == 11

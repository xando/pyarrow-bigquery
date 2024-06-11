import pytest
import pyarrow as pa
from pyarrow.bigquery.write.pa_to_pb import generate

from google.protobuf.descriptor_pb2 import FieldDescriptorProto


def test_1d():
    schema = pa.schema([("_1d_int", pa.int64())])

    descriptor = generate(schema)

    field = descriptor.fields[0]

    assert field.name == "_1d_int"
    assert field.type == FieldDescriptorProto.TYPE_INT64
    assert field.label == FieldDescriptorProto.LABEL_OPTIONAL


def test_1d_nullable_false():
    schema = pa.schema([pa.field("_1d_int", pa.int64(), nullable=False)])

    descriptor = generate(schema)

    field = descriptor.fields[0]

    assert field.name == "_1d_int"
    assert field.type == FieldDescriptorProto.TYPE_INT64
    assert field.label == FieldDescriptorProto.LABEL_REQUIRED


def test_2d():
    schema = pa.schema([("_2d_int", pa.list_(pa.int64()))])

    descriptor = generate(schema)

    field = descriptor.fields[0]

    assert field.name == "_2d_int"
    assert field.type == FieldDescriptorProto.TYPE_INT64
    assert field.label == FieldDescriptorProto.LABEL_REPEATED


def test_3d():
    schema = pa.schema([("_3d_int", pa.list_(pa.list_(pa.int64())))])

    pytest.raises(TypeError, generate, schema)


def test_struct():
    schema = pa.schema([("struct", pa.struct([("key_1", pa.string()), ("key_2", pa.string())]))])

    descriptor = generate(schema)

    assert len(descriptor.fields)
    field = descriptor.fields[0]

    assert field.name == "struct"
    assert field.type == FieldDescriptorProto.TYPE_MESSAGE
    assert field.label == FieldDescriptorProto.LABEL_OPTIONAL

    nested = field.message_type

    assert nested.fields[0].name == "key_1"
    assert nested.fields[0].type == FieldDescriptorProto.TYPE_STRING
    assert nested.fields[0].label == FieldDescriptorProto.LABEL_OPTIONAL

    assert nested.fields[1].name == "key_2"
    assert nested.fields[1].type == FieldDescriptorProto.TYPE_STRING
    assert nested.fields[1].label == FieldDescriptorProto.LABEL_OPTIONAL

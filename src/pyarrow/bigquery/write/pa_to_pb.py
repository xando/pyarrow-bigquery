import logging
import time
import random
import string

from google.protobuf.descriptor_pb2 import DescriptorProto, FieldDescriptorProto
from google.protobuf import descriptor_pb2, descriptor_pool, message_factory

import pyarrow as pa
from .type_mapping import TYPES_MAPPING
from ..some_itertools import first

logger = logging.getLogger(__name__)


FORCE_STRING = [
    pa.types.is_date,
    pa.types.is_decimal,
    pa.types.is_time,
    pa.types.is_timestamp,
]

GRPC_UPLOAD_LIMIT = 10485760


def random_string(length):
    characters = string.ascii_letters + string.digits
    return "".join(random.choices(characters, k=length))


def emit(schema, message_name):
    message_descriptor = DescriptorProto()
    message_descriptor.name = message_name

    for idx, field in enumerate(schema, 1):
        _optional = field.nullable
        _repeated = False
        _type = field.type

        if pa.types.is_list(_type):
            _repeated = True
            _type = field.type.value_type

            if pa.types.is_list(field.type.value_type):
                raise TypeError("Nested lists are not supported")

        if pa.types.is_struct(_type):
            proto_type = f"{message_name}_{field.name}"

            message_descriptor.nested_type.extend([emit(schema=_type, message_name=proto_type)])

            label = (
                FieldDescriptorProto.LABEL_REPEATED
                if _repeated
                else FieldDescriptorProto.LABEL_OPTIONAL
                if _optional
                else FieldDescriptorProto.LABEL_REQUIRED
            )

            message_descriptor.field.add(
                name=field.name,
                number=idx,
                label=label,
                type=FieldDescriptorProto.TYPE_MESSAGE,
                type_name=proto_type,
            )

        elif type_check := first(TYPES_MAPPING, lambda type_check: type_check(_type)):
            label = (
                FieldDescriptorProto.LABEL_REPEATED
                if _repeated
                else FieldDescriptorProto.LABEL_OPTIONAL
                if _optional
                else FieldDescriptorProto.LABEL_REQUIRED
            )
            message_descriptor.field.add(
                name=field.name,
                number=idx,
                label=label,
                type=TYPES_MAPPING[type_check].pb,
            )

        else:
            raise TypeError(f"Unsupported type {_type}")

    return message_descriptor


def generate(schema):
    # NOTE. (I think)
    #
    # 1)    Since we are using the same descriptor pool,
    #       we need to make sure that the message name is unique.
    # 2)    Above applies to files added to the pool as well.

    message_name = f"Message_{random_string(10)}"
    file_name = f"{random_string(10)}.proto"

    message_type = emit(schema, message_name=message_name)

    pool = descriptor_pool.Default()
    pool.AddSerializedFile(
        descriptor_pb2.FileDescriptorProto(name=file_name, message_type=[message_type]).SerializeToString()
    )

    return pool.FindMessageTypeByName(message_name)


def ammend_schema(schema):
    def _cast_field(field):
        if pa.types.is_struct(field.type):
            new_fields = [_cast_field(sub_field) for sub_field in field.type]
            return pa.field(field.name, pa.struct(new_fields))

        elif pa.types.is_list(field.type):
            new_value_field = _cast_field(field.type.value_field)
            return pa.field(field.name, pa.list_(new_value_field.type))

        elif any(type_check(field.type) for type_check in FORCE_STRING):
            return pa.field(field.name, pa.string())
        else:
            return field

    new_fields = [_cast_field(field) for field in schema]
    return pa.schema(new_fields)


def serialize(pa_table, protobuf_definition):
    Message = message_factory.GetMessageClass(protobuf_definition)

    pa_table = pa_table.cast(ammend_schema(pa_table.schema))

    rows = []
    size = 0

    for element in pa_table.to_pylist():
        t0 = time.time()
        message = Message(**element)
        size += message.ByteSize()

        logger.debug(f"Time taken to serialize: {(time.time() - t0):.4f}")

        if size > GRPC_UPLOAD_LIMIT:
            assert rows, "Row is too large to fit in a single request"
            yield rows
            rows = []

        rows.append(message.SerializeToString())

    if rows:
        yield rows

import collections

import pyarrow as pa
from google.protobuf.descriptor_pb2 import FieldDescriptorProto
from google.cloud import bigquery


TypeMapping = collections.namedtuple("TypeMapping", ["bq", "pb"])


TYPES_MAPPING = {
    pa.types.is_binary: TypeMapping(bigquery.SqlTypeNames.BYTES, FieldDescriptorProto.TYPE_BYTES),
    pa.types.is_boolean: TypeMapping(bigquery.SqlTypeNames.BOOLEAN, FieldDescriptorProto.TYPE_BOOL),
    pa.types.is_date: TypeMapping(bigquery.SqlTypeNames.DATE, FieldDescriptorProto.TYPE_STRING),
    pa.types.is_decimal: TypeMapping(bigquery.SqlTypeNames.DECIMAL, FieldDescriptorProto.TYPE_STRING),
    pa.types.is_floating: TypeMapping(bigquery.SqlTypeNames.FLOAT64, FieldDescriptorProto.TYPE_DOUBLE),
    pa.types.is_integer: TypeMapping(bigquery.SqlTypeNames.INT64, FieldDescriptorProto.TYPE_INT64),
    pa.types.is_string: TypeMapping(bigquery.SqlTypeNames.STRING, FieldDescriptorProto.TYPE_STRING),
    pa.types.is_large_string: TypeMapping(bigquery.SqlTypeNames.STRING, FieldDescriptorProto.TYPE_STRING),
    pa.types.is_time: TypeMapping(bigquery.SqlTypeNames.TIME, FieldDescriptorProto.TYPE_STRING),
    pa.types.is_timestamp: TypeMapping(bigquery.SqlTypeNames.TIMESTAMP, FieldDescriptorProto.TYPE_STRING),
}

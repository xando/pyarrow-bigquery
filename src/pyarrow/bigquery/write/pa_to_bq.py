import pyarrow as pa

from google.cloud import bigquery

from .type_mapping import TYPES_MAPPING
from ..some_itertools import first


def emit(schema):
    fields = []

    for field in schema:
        field_mode = "NULLABLE" if field.nullable else "REQUIRED"
        field_type = field.type

        if pa.types.is_list(field_type):
            field_mode = "REPEATED"
            field_type = field.type.value_type

            if pa.types.is_list(field.type.value_type):
                raise TypeError("Nested lists are not supported")

        if pa.types.is_struct(field_type):
            fields.append(
                bigquery.SchemaField(
                    name=field.name,
                    field_type="RECORD",
                    mode=field_mode,
                    fields=emit(field_type),
                )
            )

        elif type_check := first(TYPES_MAPPING, lambda type_check: type_check(field_type)):
            fields.append(
                bigquery.SchemaField(
                    name=field.name,
                    field_type=TYPES_MAPPING[type_check].bq,
                    mode=field_mode,
                )
            )
        else:
            raise TypeError(f"Unsupported type {field_type}")

    return fields


def generate(schema):
    return emit(schema)

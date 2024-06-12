import tenacity

from google.cloud.bigquery_storage_v1 import types
from google.api_core.exceptions import Unknown

from . import pa_to_pb


@tenacity.retry(stop=tenacity.stop_after_attempt(5), retry=tenacity.retry_if_exception_type(Unknown))
def _send(stream, serialized_rows, offset):
    proto_rows = types.ProtoRows()
    proto_rows.serialized_rows.extend(serialized_rows)

    proto_data = types.AppendRowsRequest.ProtoData()
    proto_data.rows = proto_rows

    request = types.AppendRowsRequest()
    request.offset = offset
    request.proto_rows = proto_data

    stream.append_rows_stream.send(request).result()


def upload_data(stream, pa_table, protobuf_definition, offset):
    local_offset = 0
    for serialized_rows in pa_to_pb.serialize(pa_table, protobuf_definition):
        _send(stream, serialized_rows, offset + local_offset)
        local_offset += len(serialized_rows)

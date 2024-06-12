from google.cloud.bigquery_storage_v1 import types
from google.api_core.exceptions import Unknown
from google.api_core import retry

from . import pa_to_pb


def upload_data(stream, pa_table, protobuf_definition, offset):
    local_offset = 0
    for serialized_rows in pa_to_pb.serialize(pa_table, protobuf_definition):
        proto_rows = types.ProtoRows()
        proto_rows.serialized_rows.extend(serialized_rows)

        proto_data = types.AppendRowsRequest.ProtoData()
        proto_data.rows = proto_rows

        request = types.AppendRowsRequest()
        request.offset = offset + local_offset
        request.proto_rows = proto_data

        job = stream.append_rows_stream.send(request)
        job.result(
            retry=retry.Retry(predicate=retry.if_exception_type(Unknown))
        )

        local_offset += len(serialized_rows)

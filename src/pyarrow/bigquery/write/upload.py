from collections import deque

import tenacity
from google.api_core.exceptions import (
    DeadlineExceeded,
    InternalServerError,
    ResourceExhausted,
    ServiceUnavailable,
    Unknown,
)
from google.cloud.bigquery_storage_v1 import types

from . import pa_to_pb

# Bound unacknowledged protobuf buffers and stop waiting on an unresponsive RPC.
_MAX_IN_FLIGHT_REQUESTS = 4
_APPEND_TIMEOUT = 300.0

_RETRYABLE_EXCEPTIONS = (
    Unknown,
    DeadlineExceeded,
    ServiceUnavailable,
    InternalServerError,
    ResourceExhausted,
)


@tenacity.retry(
    stop=tenacity.stop_after_attempt(5),
    retry=tenacity.retry_if_exception_type(_RETRYABLE_EXCEPTIONS),
)
def _send(stream, serialized_rows, offset):
    proto_rows = types.ProtoRows()
    proto_rows.serialized_rows.extend(serialized_rows)

    proto_data = types.AppendRowsRequest.ProtoData()
    proto_data.rows = proto_rows

    request = types.AppendRowsRequest()
    request.offset = offset
    request.proto_rows = proto_data

    return stream.append_rows_stream.send(request)


def _acknowledge(future, expected_offset):
    response = future.result(timeout=_APPEND_TIMEOUT)
    if response.row_errors:
        raise RuntimeError(f"AppendRows row errors: {response.row_errors}")
    protobuf = types.AppendRowsResponse.pb(response)
    result = protobuf.WhichOneof("response")
    if result == "error":
        raise RuntimeError(
            f"AppendRows failed (code {response.error.code}): {response.error.message}"
        )
    if result != "append_result" or not protobuf.append_result.HasField("offset"):
        raise RuntimeError("AppendRows returned no acknowledged offset")
    if response.append_result.offset != expected_offset:
        raise RuntimeError(
            f"AppendRows acknowledged offset {response.append_result.offset}, "
            f"expected {expected_offset}"
        )


def upload_data(stream, pa_table, protobuf_definition, offset):
    pending = deque()
    local_offset = 0
    for serialized_rows in pa_to_pb.serialize(pa_table, protobuf_definition):
        if len(pending) == _MAX_IN_FLIGHT_REQUESTS:
            _acknowledge(*pending.popleft())
        request_offset = offset + local_offset
        pending.append((_send(stream, serialized_rows, request_offset), request_offset))
        local_offset += len(serialized_rows)
    while pending:
        _acknowledge(*pending.popleft())

import pyarrow as pa

from pyarrow.bigquery.exchange.arrow_ipc import ArrowIpc


def test_arrow_ipc_roundtrip():
    exchange = ArrowIpc()
    original = pa.table({"x": [1, 2, 3], "y": ["a", "b", "c"]})
    key = exchange.store(original)
    loaded = exchange.load(key)
    assert loaded.equals(original)

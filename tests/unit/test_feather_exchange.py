import gc
import os

import pyarrow as pa

from pyarrow.bigquery.exchange.feather import Feather


def test_feather_load_memory_maps_and_unlinks_after_table_is_released():
    exchange = Feather()
    original = pa.table({"x": list(range(10_000))})
    key = exchange.store(original)

    loaded = exchange.load(key)
    assert loaded.num_rows == original.num_rows
    assert os.path.exists(key)

    del loaded
    gc.collect()
    assert not os.path.exists(key)

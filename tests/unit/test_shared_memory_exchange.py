import multiprocessing as mp
import os

import pyarrow as pa

from pyarrow.bigquery.exchange.shared_memory import SharedMemory, _parse_key


def _worker_store(queue: mp.Queue, num_rows: int) -> None:
    exchange = SharedMemory()
    table = pa.table({"x": list(range(num_rows))})
    queue.put(exchange.store(table))


def test_shared_memory_roundtrip_in_process():
    queue: mp.Queue = mp.Queue()
    process = mp.Process(target=_worker_store, args=(queue, 1000))
    process.start()
    process.join()
    assert process.exitcode == 0

    key = queue.get()
    assert ":" in key

    exchange = SharedMemory()
    loaded = exchange.load(key)
    assert loaded.num_rows == 1000


def test_shared_memory_load_unlinks_segment():
    exchange = SharedMemory()
    key = exchange.store(pa.table({"x": [1, 2, 3]}))
    name, _ = _parse_key(key)
    assert os.path.exists(f"/dev/shm/{name}")

    exchange.load(key)
    assert not os.path.exists(f"/dev/shm/{name}")

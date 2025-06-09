import multiprocessing
import uuid

import pyarrow as pa  # type: ignore[import]

from .base import ConcurrencyCompatible


class SharedMemoryDict(ConcurrencyCompatible):
    thread_compatible = False
    process_compatible = True

    def __init__(self):
        manager = multiprocessing.Manager()
        self.memory = manager.dict()

    def store(self, table: pa.Table) -> str:
        key = uuid.uuid4().hex
        self.memory[key] = table
        return key

    def load(self, key: str) -> pa.Table:
        data = self.memory[key]
        del self.memory[key]  # Remove the data after loading
        return data

import pyarrow as pa
import uuid

from .base import ConcurrencyCompatible


class Memory(ConcurrencyCompatible):
    thread_compatible = True
    process_compatible = False

    def __init__(self):
        self.memory = {}

    def store(self, table: pa.Table) -> str:
        key = uuid.uuid4().hex
        self.memory[key] = table
        return key

    def load(self, key: str) -> pa.Table:
        return self.memory[key]

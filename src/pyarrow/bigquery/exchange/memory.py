import pyarrow as pa
import uuid


class Memory:
    def __init__(self):
        self.memory = {}

    def store(self, table: pa.Table) -> str:
        key = uuid.uuid4().hex
        self.memory[key] = table
        return key

    def load(self, key: str) -> pa.Table:
        return self.memory[key]

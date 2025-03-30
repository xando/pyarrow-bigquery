import uuid
import multiprocessing

from multiprocessing import shared_memory
from multiprocessing import resource_tracker

import weakref
import pyarrow as pa


class SharedMemory:
    def store(self, table: pa.Table) -> str:
        sink = pa.BufferOutputStream()

        with pa.ipc.new_stream(sink, table.schema) as writer:
            writer.write_table(table)

        data = sink.getvalue().to_pybytes()

        key = uuid.uuid4().hex[:16]
        shm = shared_memory.SharedMemory(
            create=True,
            size=len(data),
            name=key,
        )
        resource_tracker.unregister(shm._name, "shared_memory")
        shm.buf[:] = data
        shm.close()

        return key

    def load(self, key: str) -> pa.Table:
        shm = shared_memory.SharedMemory(name=key)
        table = pa.ipc.open_stream(pa.py_buffer(shm.buf)).read_all()

        weakref.finalize(shm.buf, self.finalize, shm)

        return table

    def finalize(self, shm: shared_memory.SharedMemory) -> None:
        try:
            shm.close()
            shm.unlink()
        except FileNotFoundError:
            pass

from __future__ import annotations

import sys
import uuid

from multiprocessing import shared_memory
from multiprocessing import resource_tracker

import pyarrow as pa  # type: ignore[import]



class _ReadBufferWrapper:
    def __init__(self, shm: shared_memory.SharedMemory):
        self._shm = shm
        self._count = 0

    def __buffer__(self, _) -> memoryview:
        self._count += 1
        return self._shm.buf

    def __release_buffer__(self, view):
        self._count -= 1
        if self._count == 0:
            self._shm.close()
            self._shm.unlink()

    def __del__(self):
        try:
            self._shm.close()
            self._shm.unlink()
        except FileNotFoundError:
            pass


class SharedMemory:


    def __init__(self):
        if sys.version_info <= (3, 12):
            raise RuntimeError("Shared memory IPC requires Python 3.12 or later.")

    def store(self, table: pa.Table) -> str:  # type: ignore
        sink = pa.BufferOutputStream()  # type: ignore

        with pa.ipc.new_stream(sink, table.schema) as writer:  # type: ignore
            writer.write_table(table)

        data = sink.getvalue().to_pybytes()

        key = uuid.uuid4().hex[:16]
        shm = shared_memory.SharedMemory(
            create=True,
            size=len(data),
            name=key,
        )
        resource_tracker.unregister(shm._name, "shared_memory")  # type: ignore
        shm.buf[: len(data)] = data
        shm.close()

        return key

    def load(self, key: str) -> pa.Table:  # type: ignore
        shm = shared_memory.SharedMemory(name=key)
        resource_tracker.unregister(shm._name, "shared_memory")  # type: ignore

        return pa.ipc.open_stream(pa.py_buffer(_ReadBufferWrapper(shm))).read_all()  # type: ignore

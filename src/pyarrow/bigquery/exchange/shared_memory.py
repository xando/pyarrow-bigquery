from __future__ import annotations

import sys
import uuid

from multiprocessing import resource_tracker, shared_memory

import pyarrow as pa  # type: ignore[import]

from .base import ConcurrencyCompatible

PREFIX = "pyarrow_bq_"


def _parse_key(key: str) -> tuple[str, int]:
    name, sep, size_str = key.partition(":")
    if not sep:
        raise ValueError(f"Invalid shared memory key {key!r}, expected 'name:size'")
    return name, int(size_str)


class SharedMemory(ConcurrencyCompatible):
    """Transfer tables between processes via named shared memory segments.

    Keys passed through the read queue are ``{segment_name}:{byte_length}``.
    The main process copies bytes out and unlinks the segment immediately on
    load so /dev/shm does not accumulate while batches wait in the queue.
    """

    thread_compatible = False
    process_compatible = True

    def __init__(self):
        if sys.version_info < (3, 8):
            raise RuntimeError("Shared memory IPC requires Python 3.8 or later.")

    def store(self, table: pa.Table) -> str:  # type: ignore
        sink = pa.BufferOutputStream()  # type: ignore

        with pa.ipc.new_stream(sink, table.schema) as writer:  # type: ignore
            writer.write_table(table)

        data = sink.getvalue().to_pybytes()
        nbytes = len(data)

        name = f"{PREFIX}{uuid.uuid4().hex[:16]}"
        shm = shared_memory.SharedMemory(
            create=True,
            size=nbytes,
            name=name,
        )
        resource_tracker.unregister(shm._name, "shared_memory")  # type: ignore
        shm.buf[:nbytes] = data
        shm.close()

        return f"{name}:{nbytes}"

    def load(self, key: str) -> pa.Table:  # type: ignore
        name, nbytes = _parse_key(key)
        shm = shared_memory.SharedMemory(name=name)

        try:
            data = bytes(shm.buf[:nbytes])
        finally:
            shm.close()
            try:
                shm.unlink()
            except FileNotFoundError:
                pass

        return pa.ipc.open_stream(pa.py_buffer(data)).read_all()  # type: ignore

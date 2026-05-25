import os
import tempfile
import weakref

import pyarrow as pa

from .base import ConcurrencyCompatible

PREFIX = "pyarrow-bigquery-ipc-"


def _unlink(path: str) -> None:
    try:
        os.unlink(path)
    except FileNotFoundError:
        pass


class ArrowIpc(ConcurrencyCompatible):
    """Transfer tables between processes via Arrow IPC files (memory-mapped on load)."""

    thread_compatible = True
    process_compatible = True

    def store(self, table: pa.Table) -> str:
        fd, file_name = tempfile.mkstemp(prefix=PREFIX, suffix=".arrow")
        os.close(fd)

        with pa.OSFile(file_name, "wb") as sink:
            with pa.ipc.new_file(sink, table.schema) as writer:
                writer.write_table(table)

        return file_name

    def load(self, key: str) -> pa.Table:
        with pa.memory_map(key, "r") as source:
            with pa.ipc.open_file(source) as reader:
                table = reader.read_all()
        weakref.finalize(table, _unlink, key)
        return table

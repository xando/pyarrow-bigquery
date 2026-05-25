import os
import tempfile
import weakref

import pyarrow as pa
import pyarrow.feather as fa

from .base import ConcurrencyCompatible

PREFIX = "pyarrow-bigquery-"


def _unlink(path: str) -> None:
    try:
        os.unlink(path)
    except FileNotFoundError:
        pass


class Feather(ConcurrencyCompatible):
    thread_compatible = True
    process_compatible = True

    def store(self, table: pa.Table) -> str:
        fd, file_name = tempfile.mkstemp(prefix=PREFIX)
        os.close(fd)

        with open(file_name, "wb") as f:
            fa.write_feather(table, f)

        return file_name

    def load(self, key: str) -> pa.Table:
        table = fa.read_table(key, memory_map=True)
        weakref.finalize(table, _unlink, key)
        return table

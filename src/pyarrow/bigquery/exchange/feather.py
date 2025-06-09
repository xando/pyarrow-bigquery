import os
import tempfile
import pyarrow as pa
import threading
import pyarrow.feather as fa

from .base import ConcurrencyCompatible

PREFIX = "pyarrow-bigquery-"


class Feather(ConcurrencyCompatible):
    thread_compatible = True
    process_compatible = True

    def store(self, table: pa.Table) -> str:
        _, file_name = tempfile.mkstemp(prefix=PREFIX)

        with open(file_name, "wb") as f:
            fa.write_feather(table, f)

        return file_name

    def load(self, key: str) -> pa.Table:
        with open(key, "rb") as f:
            table = fa.read_table(f)
            threading.Thread(target=os.unlink, args=(key,)).start()
            return table

import os
import tempfile
import pyarrow as pa
import threading
import pyarrow.feather as fa


PREFIX = "pyarrow-bigquery-"


class Feather:
    def store(self, table: pa.Table) -> str:

        _, file_name = tempfile.mkstemp(prefix=PREFIX)

        with open(file_name, "wb") as f:
            fa.write_feather(table, f)

        return file_name

    def load(self, file_name: str) -> pa.Table:
        with open(file_name, "rb") as f:
            table = fa.read_table(f)
            threading.Thread(target=os.unlink, args=(file_name,)).start()
            return table

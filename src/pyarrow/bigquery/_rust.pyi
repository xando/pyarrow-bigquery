"""Python interface exported by the bundled Rust extension."""

import pyarrow as pa

def build_info() -> str: ...

class PyReader:
    def __init__(
        self,
        source: str,
        project: str,
        *,
        columns: list[str] | None = None,
        row_restrictions: str | None = None,
        max_stream_count: int = 10,
        compression: str | None = None,
        batch_size: int = 100,
        channel_capacity: int = 64,
    ) -> None: ...
    def schema_ipc(self) -> bytes: ...
    def __iter__(self) -> PyReader: ...
    def __next__(self) -> list[pa.RecordBatch]: ...

class PyWriter:
    def __init__(
        self,
        parent: str,
        schema: pa.Schema,
        worker_count: int = 10,
    ) -> None: ...
    def write_batch(self, batch: pa.RecordBatch) -> None: ...
    def close(self, commit: bool = True) -> None: ...

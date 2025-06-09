import typing
import pyarrow as pa


class ConcurrencyCompatible(typing.Protocol):
    thread_compatible: typing.ClassVar[bool] = False
    process_compatible: typing.ClassVar[bool] = False

    def store(self, table: pa.Table) -> str: ...

    def load(self, key: str) -> pa.Table: ...

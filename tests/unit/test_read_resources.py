import importlib

import pyarrow as pa
import pytest
from google.cloud.exceptions import NotFound


def test_bq_table_exists_closes_transport(monkeypatch):
    read_mod = importlib.import_module("pyarrow.bigquery.read")

    closed = []

    class FakeTransport:
        def close(self):
            closed.append(True)

    class FakeClient:
        transport = FakeTransport()

        def get_table(self, location):
            return object()

    monkeypatch.setattr(read_mod.bigquery, "Client", lambda **kwargs: FakeClient())

    read_mod._bq_table_exists("project", "project.dataset.table")

    assert closed == [True]


def test_bq_table_exists_closes_transport_on_not_found(monkeypatch):
    read_mod = importlib.import_module("pyarrow.bigquery.read")

    closed = []

    class FakeTransport:
        def close(self):
            closed.append(True)

    class FakeClient:
        transport = FakeTransport()

        def get_table(self, location):
            raise NotFound(location)

    monkeypatch.setattr(read_mod.bigquery, "Client", lambda **kwargs: FakeClient())

    with pytest.raises(NotFound):
        read_mod._bq_table_exists("project", "project.dataset.table")

    assert closed == [True]


def test_reader_deletes_source_on_exit(monkeypatch):
    """`delete_source_on_exit=True` triggers `_bq_delete_table` in `__exit__`,
    regardless of whether iteration happens. Stubs the Rust reader so no
    actual gRPC connection is made.
    """
    read_mod = importlib.import_module("pyarrow.bigquery.read")

    deleted = []

    class FakePyReader:
        def __init__(self, *args, **kwargs):
            self._schema_ipc = pa.schema([("x", pa.int64())]).serialize().to_pybytes()

        def schema_ipc(self):
            return self._schema_ipc

        def __iter__(self):
            return self

        def __next__(self):
            raise StopIteration

    monkeypatch.setattr(
        read_mod,
        "_bq_delete_table",
        lambda project, location: deleted.append((project, location)),
    )
    monkeypatch.setattr(read_mod, "_bq_table_exists", lambda *args, **kwargs: None)
    monkeypatch.setattr(read_mod._rust, "PyReader", FakePyReader)

    with read_mod.reader(
        "project.dataset.table",
        worker_count=1,
        delete_source_on_exit=True,
    ):
        pass

    assert deleted == [("project", "project.dataset.table")]

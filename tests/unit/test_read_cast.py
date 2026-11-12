import importlib
from types import SimpleNamespace

import pyarrow as pa
import pytest

read_mod = importlib.import_module("pyarrow.bigquery.read")


@pytest.fixture
def mock_read(monkeypatch):
    """Feed real Arrow IPC through Python workers and batches through Rust's wrapper."""

    def configure(table):
        class FakeReadClient:
            def read_rows(self, stream):
                for batch in table.to_batches():
                    yield SimpleNamespace(
                        arrow_record_batch=SimpleNamespace(
                            serialized_record_batch=batch.serialize()
                        )
                    )

        class FakePyReader:
            def __init__(self, *args, **kwargs):
                self.batches = iter([table.to_batches()] if table.num_rows else [])

            def schema_ipc(self):
                return table.schema.serialize()

            def __next__(self):
                return next(self.batches)

        class FakeQueryClient:
            def query(self, query, **kwargs):
                return SimpleNamespace(
                    result=lambda: None,
                    destination=SimpleNamespace(project="p", dataset_id="d", table_id="t"),
                )

        monkeypatch.setattr(read_mod, "_bq_table_exists", lambda *args: None)
        monkeypatch.setattr(
            read_mod,
            "_bq_read_create_streams",
            lambda **kwargs: (["stream"] if table.num_rows else [], table.schema),
        )
        monkeypatch.setattr(read_mod.bigquery_storage, "BigQueryReadClient", FakeReadClient)
        monkeypatch.setattr(read_mod._rust, "PyReader", FakePyReader)
        monkeypatch.setattr(read_mod.bigquery, "Client", lambda **kwargs: FakeQueryClient())

    return configure


@pytest.mark.parametrize("engine", ["python", "rust"])
@pytest.mark.parametrize("api", ["reader", "reader_query", "read_table", "read_query"])
@pytest.mark.parametrize("empty", [False, True])
@pytest.mark.parametrize("cast_by", ["cast_column", "cast_type"])
def test_cast_across_read_apis(mock_read, engine, api, empty, cast_by):
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False, metadata={b"field": b"metadata"}),
            pa.field("values", pa.list_(pa.int64())),
            pa.field("unchanged", pa.string()),
        ],
        metadata={b"schema": b"metadata"},
    )
    source = pa.Table.from_pydict(
        {
            "id": [1, 2, 3, 4, 5],
            "values": [[1, None], None, [], [2], [3]],
            "unchanged": ["a", None, "b", "c", "d"],
        },
        schema=schema,
    )
    if empty:
        source = source.slice(0, 0)
    mock_read(source)
    casts = (
        {"id": pa.int32(), "values": pa.list_(pa.float64())}
        if cast_by == "cast_column"
        else {pa.int64(): pa.int32(), pa.list_(pa.int64()): pa.list_(pa.float64())}
    )
    kwargs = dict(engine=engine, batch_size=2, worker_count=1, **{cast_by: casts})
    args = ("p", "SELECT * FROM t") if "query" in api else ("p.d.t",)
    result = getattr(read_mod, api)(*args, **kwargs)
    expected_schema = schema.set(0, schema.field(0).with_type(pa.int32()))
    expected_schema = expected_schema.set(1, schema.field(1).with_type(pa.list_(pa.float64())))
    if api.startswith("reader"):
        with result as stream:
            assert stream.schema.equals(expected_schema, check_metadata=True)
            chunks = list(stream)
            assert [chunk.num_rows for chunk in chunks] == ([] if empty else [2, 2, 1])
            result = pa.concat_tables(chunks) if chunks else stream.schema.empty_table()
    assert result.schema.equals(expected_schema, check_metadata=True)
    assert result.equals(source.cast(expected_schema))
    assert source.schema.equals(schema, check_metadata=True)


@pytest.mark.parametrize("engine", ["python", "rust"])
@pytest.mark.parametrize("cast_by", ["cast_column", "cast_type"])
@pytest.mark.parametrize(
    ("values", "target"),
    [
        ([2**31], pa.int32()),
        ([1.5], pa.int32()),
        (["not-an-integer"], pa.int32()),
        ([[2**31]], pa.list_(pa.int32())),
        ([1], pa.list_(pa.float64())),
    ],
)
def test_cast_raises_on_unsafe_or_unsupported_conversion(
    mock_read, engine, values, target, cast_by
):
    source = pa.table({"value": values})
    mock_read(source)
    key = "value" if cast_by == "cast_column" else source.schema.field("value").type
    with pytest.raises((pa.ArrowInvalid, pa.ArrowNotImplementedError)):
        read_mod.read_table("p.d.t", engine=engine, worker_count=1, **{cast_by: {key: target}})


@pytest.mark.parametrize("engine", ["python", "rust"])
def test_cast_column_requires_column_in_read_schema(mock_read, engine):
    mock_read(pa.table({"selected": [1]}))
    with pytest.raises(KeyError, match="missing.*not in the read schema"):
        with read_mod.reader(
            "p.d.t", columns=["selected"], cast_column={"missing": pa.int32()}, engine=engine
        ):
            pass


@pytest.mark.parametrize("cast_column", [[("id", pa.int32())], {"id": "int32"}, {1: pa.int32()}])
def test_cast_column_requires_mapping_to_arrow_types(cast_column):
    with pytest.raises(TypeError, match="cast_column must"):
        read_mod.reader("p.d.t", cast_column=cast_column)


@pytest.mark.parametrize(
    "cast_type",
    [[(pa.int64(), pa.int32())], {"int64": pa.int32()}, {pa.int64(): "int32"}],
)
def test_cast_type_requires_mapping_between_arrow_types(cast_type):
    with pytest.raises(TypeError, match="cast_type must"):
        read_mod.reader("p.d.t", cast_type=cast_type)


@pytest.mark.parametrize("engine", ["python", "rust"])
def test_cast_type_uses_original_top_level_types_with_column_overrides(mock_read, engine):
    source = pa.table(
        {
            "small": pa.array([1, None], type=pa.int32()),
            "large": pa.array([2**40, None], type=pa.int64()),
            "other": pa.array([2, None], type=pa.int64()),
            "lists": pa.array([[1, None], None], type=pa.list_(pa.int64())),
            "records": pa.array([{"value": 1}, None], type=pa.struct([("value", pa.int64())])),
            "label": ["a", None],
        }
    )
    mock_read(source)
    result = read_mod.read_table(
        "p.d.t",
        engine=engine,
        worker_count=1,
        cast_type={pa.int32(): pa.int64(), pa.int64(): pa.int16(), pa.binary(): pa.string()},
        cast_column={"large": pa.int64(), "label": pa.large_string()},
    )
    assert result.schema.types == [
        pa.int64(),
        pa.int64(),
        pa.int16(),
        pa.list_(pa.int64()),
        pa.struct([("value", pa.int64())]),
        pa.large_string(),
    ]
    assert result.to_pydict() == source.to_pydict()


@pytest.mark.parametrize("engine", ["python", "rust"])
def test_cast_type_unmatched_rule_is_ignored(mock_read, engine):
    source = pa.table({"value": [2**40, None]})
    mock_read(source)
    result = read_mod.read_table(
        "p.d.t", engine=engine, worker_count=1, cast_type={pa.int32(): pa.float32()}
    )
    assert result.equals(source)


@pytest.mark.parametrize("engine", ["python", "rust"])
@pytest.mark.parametrize("cast_column", [None, {}])
@pytest.mark.parametrize("cast_type", [None, {}])
def test_no_cast_preserves_schema_and_values(mock_read, engine, cast_column, cast_type):
    source = pa.table({"id": [2**40, None], "values": [[1], []]})
    mock_read(source)
    result = read_mod.read_table(
        "p.d.t", cast_column=cast_column, cast_type=cast_type, engine=engine, worker_count=1
    )
    assert result.equals(source)


def test_rust_engine_releases_each_decoded_batch_once_cast(monkeypatch):
    """A batch is cast as it arrives and the pending chunk holds none of its decoded memory,
    neither through a column the cast leaves unchanged nor through the offsets of a list."""
    rows = 100_000
    groups = 4
    schema = pa.schema([("values", pa.list_(pa.int64())), ("flag", pa.bool_())])
    allocated = []

    class FakePyReader:
        def __init__(self, *args, **kwargs):
            self.remaining = groups

        def schema_ipc(self):
            return schema.serialize()

        def __next__(self):
            allocated.append(pa.total_allocated_bytes())
            if not self.remaining:
                raise StopIteration
            self.remaining -= 1
            # Decoded like the Rust reader's batches: every column a view of one allocation
            batch = pa.record_batch(
                [pa.repeat(list(range(10)), rows), pa.repeat(True, rows)], schema=schema
            )
            return [pa.ipc.read_record_batch(batch.serialize(), schema)]

    monkeypatch.setattr(read_mod, "_bq_table_exists", lambda *args: None)
    monkeypatch.setattr(read_mod._rust, "PyReader", FakePyReader)

    with read_mod.reader(
        "p.d.t",
        engine="rust",
        batch_size=groups * rows,
        cast_type={pa.list_(pa.int64()): pa.list_(pa.int8())},
    ) as stream:
        chunks = list(stream)

    assert [chunk.num_rows for chunk in chunks] == [groups * rows]
    assert chunks[0].schema.types == [pa.list_(pa.int8()), pa.bool_()]
    decoded_batch_bytes = rows * 10 * 8
    assert max(allocated) - allocated[0] < decoded_batch_bytes


def test_python_engine_casts_in_the_worker(mock_read):
    """The worker hands cast tables to the exchange, not tables the main process still has to cast."""

    class RecordingExchange:
        thread_compatible = True

        def __init__(self):
            self.tables = {}

        def store(self, table):
            key = str(len(self.tables))
            self.tables[key] = table
            return key

        def load(self, key):
            return self.tables[key]

    source = pa.table({"value": pa.array([1, 2, 3], type=pa.int64())})
    mock_read(source)
    ipc_exchange = RecordingExchange()

    result = read_mod.read_table(
        "p.d.t",
        engine="python",
        worker_count=1,
        batch_size=2,
        ipc_exchange=ipc_exchange,
        cast_type={pa.int64(): pa.int32()},
    )

    assert result.to_pydict() == source.to_pydict()
    assert [table.schema.types for table in ipc_exchange.tables.values()] == [[pa.int32()]] * 2

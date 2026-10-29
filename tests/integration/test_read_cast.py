"""Read-only casting checks against a small public BigQuery table.

Run with Application Default Credentials and GCP_PROJECT set to a project
where the caller can create BigQuery read sessions and query jobs.
"""

import os

import pyarrow as pa
import pytest

import pyarrow.bigquery as bq

TABLE = "bigquery-public-data.samples.shakespeare"
FILTER = "corpus = 'hamlet' AND word_count >= 100"
COLUMNS = ["word", "word_count", "corpus_date"]
PROJECT = os.environ.get("GCP_PROJECT")
pytestmark = pytest.mark.skipif(
    not PROJECT, reason="GCP_PROJECT is required for public-table reads"
)


def _read(api, engine, column_cast=None, empty=False):
    restriction = f"{FILTER} AND FALSE" if empty else FILTER
    options = dict(engine=engine, column_cast=column_cast, worker_count=1, batch_size=7)
    if "query" in api:
        query = f"SELECT {', '.join(COLUMNS)} FROM `{TABLE}` WHERE {restriction}"
        result = getattr(bq, api)(PROJECT, query, location="US", **options)
    else:
        result = getattr(bq, api)(
            TABLE, project=PROJECT, columns=COLUMNS, row_restrictions=restriction, **options
        )
    if api.startswith("reader"):
        with result as stream:
            chunks = list(stream)
            assert all(chunk.schema == stream.schema and chunk.num_rows <= 7 for chunk in chunks)
            return pa.concat_tables(chunks) if chunks else stream.schema.empty_table()
    return result


@pytest.fixture(scope="module")
def source_table():
    return _read("read_table", "python").sort_by("word")


@pytest.mark.parametrize("engine", ["python", "rust"])
@pytest.mark.parametrize("api", ["read_table", "reader", "read_query", "reader_query"])
def test_public_table_column_cast(source_table, engine, api):
    casts = {"word_count": pa.int32(), "corpus_date": pa.int16()}
    result = _read(api, engine, column_cast=casts).sort_by("word")
    assert result.num_rows > 7
    assert result.to_pydict() == source_table.to_pydict()
    assert result.schema.field("word_count").type == pa.int32()
    assert result.schema.field("corpus_date").type == pa.int16()
    assert result.schema.field("word").type == source_table.schema.field("word").type

    empty = _read(api, engine, column_cast=casts, empty=True)
    assert empty.num_rows == 0
    assert empty.schema == result.schema


@pytest.mark.parametrize("engine", ["python", "rust"])
@pytest.mark.parametrize("api", ["read_query", "reader_query"])
def test_public_query_list_cast(engine, api):
    query = f"""
        SELECT word, word_count, [word_count, word_count + 1] AS counts
        FROM `{TABLE}` WHERE {FILTER}
        ORDER BY word LIMIT 5
    """
    result = getattr(bq, api)(
        PROJECT,
        query,
        location="US",
        engine=engine,
        worker_count=1,
        batch_size=2,
        column_cast={"word_count": pa.int32(), "counts": pa.list_(pa.float64())},
    )
    if api.startswith("reader"):
        with result as stream:
            chunks = list(stream)
            assert [chunk.num_rows for chunk in chunks] == [2, 2, 1]
            assert all(chunk.schema == stream.schema for chunk in chunks)
            result = pa.concat_tables(chunks)
    assert result.num_rows == 5
    assert result.schema.field("word_count").type == pa.int32()
    assert result.schema.field("counts").type == pa.list_(pa.float64())
    for row in result.to_pylist():
        assert row["counts"] == [float(row["word_count"]), float(row["word_count"] + 1)]


@pytest.mark.parametrize("engine", ["python", "rust"])
def test_public_table_cast_overflow(engine):
    with pytest.raises(pa.ArrowInvalid, match="not in range"):
        _read("read_table", engine, column_cast={"corpus_date": pa.int8()})

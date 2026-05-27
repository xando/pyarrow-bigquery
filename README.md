
# pyarrow-bigquery

An extension library to **write** to and **read** from BigQuery tables as PyArrow tables.

## Table of Contents

- [Installation](#installation)
- [Source Code](#source-code)
- [Quick Start](#quick-start)
  - [Reading](#reading)
  - [Writing](#writing)
- [Examples](#examples)
  - [Column projection and row filters](#column-projection-and-row-filters)
  - [Query location and large results](#query-location-and-large-results)
  - [Parallel workers and IPC exchange](#parallel-workers-and-ipc-exchange)
  - [Read engines (`engine="python"` vs `"rust"`)](#read-engines-enginepython-vs-rust)
  - [Table creation options on write](#table-creation-options-on-write)
  - [Chunked write with `batch_size`](#chunked-write-with-batch_size)
- [API Reference](#api-reference)
  - [Writing](#writing-1)
    - [`pyarrow.bigquery.write_table`](#pyarrowbigquerywrite_table)
    - [`pyarrow.bigquery.writer` (context manager)](#pyarrowbigquerywriter-context-manager)
    - [`pyarrow.bigquery.writer.write_table` / `write_batch`](#pyarrowbigquerywriterwrite_table--write_batch)
  - [Reading](#reading-1)
    - [`pyarrow.bigquery.read_table`](#pyarrowbigqueryread_table)
    - [`pyarrow.bigquery.read_query`](#pyarrowbigqueryread_query)
    - [`pyarrow.bigquery.reader` (context manager)](#pyarrowbigqueryreader-context-manager)
    - [`pyarrow.bigquery.reader_query` (context manager)](#pyarrowbigqueryreader_query-context-manager)
  - [IPC exchange (`pyarrow.bigquery.exchange`)](#ipc-exchange-pyarrowbigqueryexchange)
- [Authentication](#authentication)

---

## Installation

```bash
pip install pyarrow-bigquery
```

Authenticate with Google Cloud (Application Default Credentials), for example:

```bash
gcloud auth application-default login
```

## Source Code

https://github.com/xando/pyarrow-bigquery/

## Quick Start

Import the namespace package:

```python
import pyarrow as pa
import pyarrow.bigquery as bq
```

Table and query locations use `project.dataset.table`. The project in the path is also used as the billing project unless you pass `project=` explicitly.

### Reading

| Goal | API |
|------|-----|
| Whole table in memory | `read_table` |
| Whole query result in memory | `read_query` |
| Stream a large table in chunks | `reader` (context manager) |
| Stream a large query in chunks | `reader_query` (context manager) |

**Small table — load entirely**

```python
table = bq.read_table("my_project.my_dataset.events")
print(table.num_rows, table.schema)
```

**Small query — load entirely**

```python
table = bq.read_query(
    project="my_project",
    query="SELECT id, ts FROM `my_project.my_dataset.events` WHERE ts >= '2024-01-01'",
)
```

**Large table — iterate batches**

```python
with bq.reader("my_project.my_dataset.events", batch_size=10_000) as r:
    for chunk in r:
        process(chunk)  # each chunk is a pa.Table
```

**Large query — iterate batches**

```python
with bq.reader_query(
    project="my_project",
    query="SELECT * FROM `my_project.my_dataset.events`",
    batch_size=10_000,
) as r:
    print(r.schema)
    for chunk in r:
        process(chunk)
```

### Writing

| Goal | API |
|------|-----|
| Upload a table in one call | `write_table` |
| Stream many chunks (generator, ETL, etc.) | `writer` (context manager) |

**One-shot upload**

```python
table = pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]})
bq.write_table(table, "my_project.my_dataset.names")
```

**Streaming upload**

```python
schema = pa.schema([("id", pa.int64()), ("payload", pa.string())])

with bq.writer(schema, "my_project.my_dataset.streamed") as w:
    for batch in generate_batches():
        w.write_table(batch)  # or w.write_batch(record_batch)
```

---

## Examples

### Column projection and row filters

BigQuery applies `columns` and `row_restrictions` before data is streamed to clients:

```python
table = bq.read_table(
    "my_project.my_dataset.events",
    columns=["user_id", "event_name", "ts"],
    row_restrictions="event_name = 'purchase' AND ts >= '2024-06-01'",
)
```

The same options work on `reader`, `read_query`, and `reader_query`.

### Query location and large results

Pass `location` when the query must run in a specific region. For very large query outputs, materialize into a temporary table and read via the Storage API:

```python
with bq.reader_query(
    project="my_project",
    query="SELECT * FROM huge_join ...",
    location="EU",
    large_results=True,
    batch_size=50_000,
) as r:
    for chunk in r:
        process(chunk)
# temporary result table is deleted on exit when large_results=True
```

`read_query(..., large_results=True)` works the same way but loads everything into memory.

### Parallel workers and IPC exchange

Reads and writes use a pool of threads or processes (`worker_count`, `worker_type`). For **process** workers, pass an IPC exchange compatible with multiprocessing (default: `exchange.ArrowIpc()`):

```python
import multiprocessing
import pyarrow.bigquery.exchange as exchange

with bq.reader(
    "my_project.my_dataset.big_table",
    worker_type=multiprocessing.Process,
    worker_count=8,
    ipc_exchange=exchange.ArrowIpc(),
    compression="zstd",
) as r:
    for chunk in r:
        process(chunk)
```

Thread workers default to `exchange.Memory()`. Other exchanges (`Feather`, `SharedMemory`, …) are available under `pyarrow.bigquery.exchange` for advanced tuning.

`worker_type` / `ipc_exchange` only apply to the Python read engine (the default). See below for the Rust engine, which manages concurrency internally.

### Read engines (`engine="python"` vs `"rust"`)

The read APIs (`read_table`, `read_query`, `reader`, `reader_query`) accept an `engine` parameter selecting which implementation runs the BigQuery Storage Read session.

| `engine` | Default | How it works |
|---|---|---|
| `"python"` | ✅ | Spawns a worker pool (threads or processes per `worker_type`) using the `google-cloud-bigquery-storage` Python client; results travel through a configurable `ipc_exchange`. Behaviour is identical to 0.6.x. |
| `"rust"` |   | Uses the bundled `pyarrow.bigquery._rust` extension: a native `tonic` gRPC client + tokio runtime decodes Arrow IPC and hands batches to Python over the Arrow C Data Interface (no IPC roundtrip, no IPC exchange to configure). |

```python
# Default — Python worker pool, identical to 0.6.x
table = bq.read_table("my_project.my_dataset.events")

# Opt-in Rust engine
table = bq.read_table("my_project.my_dataset.events", engine="rust")

# Works the same way for the streaming context manager
with bq.reader("my_project.my_dataset.events", engine="rust", batch_size=10_000) as r:
    for chunk in r:
        process(chunk)
```

**With `engine="rust"`:**

- `columns`, `row_restrictions`, `batch_size`, `compression` (`None | "lz4" | "zstd"`), `project`, `delete_source_on_exit` all work the same as in the Python engine.
- `worker_count` is honoured: it caps the number of read streams the session opens (`max_stream_count = worker_count * 3`, same default as the Python path).
- `worker_type` and `ipc_exchange` are **ignored** (a warning is emitted if you pass non-default values). The Rust path always uses a single shared tokio runtime; there is no thread/process choice to make and no IPC exchange to serialize through.
- Authentication uses the same Application Default Credentials chain as the Python client (`gcp_auth` crate under the hood).

**Choosing an engine:** the default is `"python"` because it's the well-tested, in-place implementation. Switch to `"rust"` if you've measured a benefit on your workload and are happy depending on the prebuilt wheel for your platform. Both engines pass the same unit-test suite and return the same row counts.

### Table creation options on write

```python
# Replace table if it already exists
bq.write_table(
    table,
    "my_project.my_dataset.snapshot",
    table_overwrite=True,
)

# Auto-expire after 7 days
bq.write_table(
    table,
    "my_project.my_dataset.temp_export",
    table_expire=7 * 24 * 3600,
)

# Append to an existing table (must already exist and match schema)
bq.write_table(
    more_rows,
    "my_project.my_dataset.events",
    table_create=False,
)
```

### Chunked write with `batch_size`

`write_table` splits the input into upload chunks (default `batch_size=10` rows per chunk):

```python
bq.write_table(large_table, "my_project.my_dataset.loaded", batch_size=5000)
```

---

## API Reference

### Writing

#### `pyarrow.bigquery.write_table`

Writes a PyArrow table to BigQuery. Returns nothing.

**Parameters:**

- `table`: `pa.Table` — must be non-empty.
- `where`: `str` — destination `project.dataset.table`.
- `project`: `str | None`, *default* `None` — billing project; inferred from `where` when omitted.
- `table_create`: `bool`, *default* `True` — create the destination table if missing.
- `table_expire`: `int | None`, *default* `None` — seconds until table expiry (only when creating).
- `table_overwrite`: `bool`, *default* `False` — delete and recreate the table if it exists.
- `worker_type`: `threading.Thread | multiprocessing.Process`, *default* `threading.Thread`.
- `worker_count`: `int`, *default* `os.cpu_count()`.
- `batch_size`: `int`, *default* `10` — rows per upload chunk inside the call.

```python
bq.write_table(table, "my_project.my_dataset.out")
```

#### `pyarrow.bigquery.writer` (context manager)

Incremental writes. Constructor: `writer(schema, where, **options)`.

**Parameters:** same table-creation and worker options as `write_table` (no `batch_size` on the context manager itself).

**Methods:**

- `write_table(table: pa.Table)` — enqueue a table chunk.
- `write_batch(batch: pa.RecordBatch)` — enqueue a single record batch.

```python
schema = pa.schema([("n", pa.int64())])

with bq.writer(schema, "my_project.my_dataset.incremental") as w:
    w.write_batch(pa.record_batch([pa.array([1, 2])], schema=schema))
    w.write_table(pa.table({"n": [3, 4, 5]}))
```

#### `pyarrow.bigquery.writer.write_table` / `write_batch`

See `writer` above. Typical loop:

```python
with bq.writer(schema, "my_project.my_dataset.rows") as w:
    for i in range(1000):
        w.write_table(pa.table({"value": [i] * 10}, schema=schema))
```

### Reading

#### `pyarrow.bigquery.read_table`

Loads a full table into memory (`pa.concat_tables` over internal batches).

**Parameters:**

- `source`: `str` — `project.dataset.table`.
- `project`: `str | None`, *default* `None`.
- `columns`: `list[str] | None`, *default* `None` — subset of columns; all columns when omitted.
- `row_restrictions`: `str | None`, *default* `None` — SQL filter pushed down to BigQuery Storage Read.
- `worker_type`, `worker_count` — parallel fetch backend (Python engine only; `worker_count` still caps stream count under the Rust engine).
- `ipc_exchange`: exchange instance for worker handoff; default `Memory` (threads) or `ArrowIpc` (processes). Ignored when `engine="rust"`.
- `batch_size`: `int`, *default* `100` — target rows per internal chunk.
- `compression`: `None | "lz4" | "zstd"`, *default* `None` — Storage API Arrow compression.
- `engine`: `"python" | "rust"`, *default* `"python"` — implementation selector; see [Read engines](#read-engines-enginepython-vs-rust).

```python
table = bq.read_table("my_project.my_dataset.events", columns=["id"], batch_size=500)
```

#### `pyarrow.bigquery.read_query`

Runs a query and returns the full result as one `pa.Table`. Accepts the same read tuning parameters as `read_table` (including `engine`), plus:

- `location`: `str | None` — query job location.
- `large_results`: `bool`, *default* `False` — materialize to a temp table, then read (deleted on exit).
- `large_results_dataset`: `str`, *default* `"_temp_pyarrow_bigquery"`.
- `large_results_expiration_ms`: `int`, *default* `86400000` — dataset default TTL when the dataset is first created.

```python
table = bq.read_query(
    project="my_project",
    query="SELECT id FROM `my_project.my_dataset.events` LIMIT 1000",
    location="US",
)
```

#### `pyarrow.bigquery.reader` (context manager)

Streams a table as an iterator of `pa.Table` chunks.

**Parameters:** same as `read_table` (including `engine`).

**Attributes:**

- `schema`: `pa.Schema` — available after `__enter__`.

```python
parts = []
with bq.reader("my_project.my_dataset.events", batch_size=2000) as r:
    print(r.schema)
    for chunk in r:
        parts.append(chunk)
full = pa.concat_tables(parts) if parts else pa.table({})
```

#### `pyarrow.bigquery.reader_query` (context manager)

Runs a query, then streams the result like `reader`. Query-related parameters match `read_query` (including `engine`).

```python
with bq.reader_query(
    project="my_project",
    query="SELECT * FROM `my_project.my_dataset.events`",
    large_results=True,
) as r:
    for chunk in r:
        print(chunk.num_rows)
```

### IPC exchange (`pyarrow.bigquery.exchange`)

Workers pass Arrow tables through a pluggable exchange. Implementations set `thread_compatible` / `process_compatible` flags:

| Class | Threads | Processes | Notes |
|-------|---------|-----------|--------|
| `Memory` | yes | no | Default for thread workers |
| `ArrowIpc` | no | yes | Default for process workers; mmap temp files |
| `Feather` | no | yes | Feather temp files |
| `SharedMemory` | no | yes | POSIX shared memory |
| `SharedMemoryDict` | yes | yes | Slow; mainly for completeness |

```python
import pyarrow.bigquery.exchange as exchange

with bq.reader(
    "my_project.my_dataset.t",
    worker_type=multiprocessing.Process,
    ipc_exchange=exchange.SharedMemory(),
) as r:
    ...
```

---

## Authentication

The library uses [Application Default Credentials](https://cloud.google.com/docs/authentication/application-default-credentials). Ensure the active principal can read/write the target datasets (BigQuery and BigQuery Storage API permissions).

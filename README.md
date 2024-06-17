

# pyarrow-bigquery

A simple library to **write to** and **download from** BigQuery tables as PyArrow tables.

---

## Installation

```bash
pip install pyarrow-bigquery
```

---

## Quick Start

This guide will help you quickly get started with `pyarrow-bigquery`, a library that allows you to **read** from and **write** to Google BigQuery using PyArrow.

### Reading

`pyarrow-bigquery` offers four methods to read BigQuery tables as PyArrow tables. Depending on your use case and/or the table size, you can choose the most suitable method.

**Read from a Table Location**

When the table is small enough to fit in memory, you can read it directly using `read_table`.

```python
import pyarrow.bigquery as bq

table = bq.read_table("gcp_project.dataset.small_table")

print(table.num_rows)
```

**Read from a Query**

Alternatively, if the query results are small enough to fit in memory, you can read them directly using `read_query`.

```python
import pyarrow.bigquery as bq

table = bq.read_query(
    project="gcp_project",
    query="SELECT * FROM `gcp_project.dataset.small_table`"
)

print(table.num_rows)
```

**Read in Batches**

If the target table is larger than memory or you prefer not to fetch the entire table at once, you can use the `bq.reader` iterator method with the `batch_size` parameter to limit how much data is fetched per iteration.

```python
import pyarrow.bigquery as bq

for table in bq.reader("gcp_project.dataset.big_table", batch_size=100):
    print(table.num_rows)
```

**Read Query in Batches**

Similarly, you can read data in batches from a query using `reader_query`.

```python
import pyarrow.bigquery as bq

for table in bq.reader_query(
    project="gcp_project",
    query="SELECT * FROM `gcp_project.dataset.small_table`"
):
    print(table.num_rows)
```

### Writing

The package provides two methods to write to BigQuery. Depending on your use case or the table size, you can choose the appropriate method.

**Write the Entire Table**

To write a complete table at once, use the `bq.write_table` method.

```python
import pyarrow as pa
import pyarrow.bigquery as bq

table = pa.Table.from_arrays([[1, 2, 3, 4]], names=['integers'])

bq.write_table(table, 'gcp_project.dataset.table')
```

**Write in Batches**

If you need to write data in smaller chunks, use the `bq.writer` method with the `schema` parameter to define the table structure.

```python
import pyarrow as pa
import pyarrow.bigquery as bq

schema = pa.schema([
    ("integers", pa.int64())
])

with bq.writer("gcp_project.dataset.table", schema=schema) as writer:
    writer.write_batch(record_batch)
    writer.write_table(table)
```

---

## API Reference

### Writing

#### `pyarrow.bigquery.write_table`

Writes a PyArrow Table to a BigQuery Table. No return value.

**Parameters:**

- `table`: `pa.Table`  
  The PyArrow table.

- `where`: `str`  
  The destination location in the BigQuery catalog.

- `project`: `str`, *default* `None`  
  The BigQuery execution project, also the billing project. If not provided, it will be extracted from `where`.

- `table_create`: `bool`, *default* `True`  
  Specifies if the BigQuery table should be created.

- `table_expire`: `None | int`, *default* `None`  
  The number of seconds after which the created table will expire. Used only if `table_create` is `True`. Set to `None` to disable expiration.

- `table_overwrite`: `bool`, *default* `False`  
  If the table already exists, it will be destroyed and a new one will be created.

- `worker_type`: `threading.Thread | multiprocessing.Process`, *default* `threading.Thread`  
  The worker backend for fetching data.

- `worker_count`: `int`, *default* `os.cpu_count()`  
  The number of threads or processes to use for fetching data from BigQuery.

- `batch_size`: `int`, *default* `100`  
  The batch size for fetched rows.

```python
bq.write_table(table, 'gcp_project.dataset.table')
```

#### `pyarrow.bigquery.writer` (Context Manager)

Context manager version of the write method. Useful when the PyArrow table is larger than memory size or the table is available in chunks.

**Parameters:**

- `schema`: `pa.Schema`  
  The PyArrow schema.

- `where`: `str`  
  The destination location in the BigQuery catalog.

- `project`: `str`, *default* `None`  
  The BigQuery execution project, also the billing project. If not provided, it will be extracted from `where`.

- `table_create`: `bool`, *default* `True`  
  Specifies if the BigQuery table should be created.

- `table_expire`: `None | int`, *default* `None`  
  The number of seconds after which the created table will expire. Used only if `table_create` is `True`. Set to `None` to disable expiration.

- `table_overwrite`: `bool`, *default* `False`  
  If the table already exists, it will be destroyed and a new one will be created.

- `worker_type`: `threading.Thread | multiprocessing.Process`, *default* `threading.Thread`  
  The worker backend for writing data.

- `worker_count`: `int`, *default* `os.cpu_count()`  
  The number of threads or processes to use for writing data to BigQuery.

- `batch_size`: `int`, *default* `100`  
  The batch size used for writes. The table will be automatically split to this value.

Depending on your use case, you might want to use one of the methods below to write your data to a BigQuery table, using either `pa.Table` or `pa.RecordBatch`.

#### `pyarrow.bigquery.writer.write_table` (Context Manager Method)

Context manager method to write a table.

**Parameters:**

- `table`: `pa.Table`  
  The PyArrow table.

```python
import pyarrow as pa
import pyarrow.bigquery as bq

schema = pa.schema([("value", pa.list_(pa.int64()))])

with bq.writer("gcp_project.dataset.table", schema=schema) as writer:
    for a in range(1000):
        writer.write_table(pa.Table.from_pylist([{'value': [a] * 10}]))
```

#### `pyarrow.bigquery.writer.write_batch` (Context Manager Method)

Context manager method to write a record batch.

**Parameters:**

- `batch`: `pa.RecordBatch`  
  The PyArrow record batch.

```python
import pyarrow as pa
import pyarrow.bigquery as bq

schema = pa.schema([("value", pa.list_(pa.int64()))])

with bq.writer("gcp_project.dataset.table", schema=schema) as writer:
    for a in range 1000:
        writer.write_batch(pa.RecordBatch.from_pylist([{'value': [1] * 10}]))
```

### Reading

#### `pyarrow.bigquery.read_table`

**Parameters:**

- `source`: `str`  
  The BigQuery table location.

- `project`: `str`, *default* `None`  
  The BigQuery execution project, also the billing project. If not provided, it will be extracted from `source`.

- `columns`: `str`, *default* `None`  
  The columns to download. When not provided, all available columns will be downloaded.

- `row_restrictions`: `str`, *default* `None`  
  Row-level filtering executed on the BigQuery side. More information is available in the [BigQuery documentation](https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1beta1).

- `worker_type`: `threading.Thread | multiprocessing.Process`, *default* `threading.Thread`  
  The worker backend for fetching data.

- `worker_count`: `int`, *default* `os.cpu_count()`  
  The number of threads or processes to use for fetching data from BigQuery.

- `batch_size`: `int`, *default* `100`  
  The batch size used for fetching. The table will be automatically split into this value.

#### `pyarrow.bigquery.read_query`

**Parameters:**

- `project`: `str`  
  The BigQuery query execution (and billing) project.

- `query`: `str`  
  The query to be executed.

- `worker_type`: `threading.Thread | multiprocessing.Process`, *default* `threading.Thread`  
  The worker backend for fetching data.

- `worker_count`: `int`, *default* `os.cpu_count()`  
  The number of threads or processes to use for fetching data from BigQuery.

- `batch_size`: `int`, *default* `100`  
  The batch size used for fetching. The table will be automatically split into this value.

```python
table = bq.read_query("gcp_project", "SELECT * FROM `gcp_project.dataset.table`")
```

#### `pyarrow.bigquery.reader`

**Parameters:**

- `

source`: `str`  
  The BigQuery table location.

- `project`: `str`, *default* `None`  
  The BigQuery execution project, also the billing project. If not provided, it will be extracted from `source`.

- `columns`: `str`, *default* `None`  
  The columns to download. When not provided, all available columns will be downloaded.

- `row_restrictions`: `str`, *default* `None`  
  Row-level filtering executed on the BigQuery side. More information is available in the [BigQuery documentation](https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1beta1).

- `worker_type`: `threading.Thread | multiprocessing.Process`, *default* `threading.Thread`  
  The worker backend for fetching data.

- `worker_count`: `int`, *default* `os.cpu_count()`  
  The number of threads or processes to use for fetching data from BigQuery.

- `batch_size`: `int`, *default* `100`  
  The batch size used for fetching. The table will be automatically split into this value.

```python
import pyarrow as pa
import pyarrow.bigquery as bq

parts = []
for part in bq.reader("gcp_project.dataset.table"):
    parts.append(part)

table = pa.concat_tables(parts)
```

#### `pyarrow.bigquery.reader_query`

**Parameters:**

- `project`: `str`  
  The BigQuery query execution (and billing) project.

- `query`: `str`  
  The query to be executed.

- `worker_type`: `threading.Thread | multiprocessing.Process`, *default* `threading.Thread`  
  The worker backend for fetching data.

- `worker_count`: `int`, *default* `os.cpu_count()`  
  The number of threads or processes to use for fetching data from BigQuery.

- `batch_size`: `int`, *default* `100`  
  The batch size used for fetching. The table will be automatically split into this value.

```python
for batch in bq.reader_query("gcp_project", "SELECT * FROM `gcp_project.dataset.table`"):
    print(batch.num_rows)
```

# pyarrow-bigquery

A simple library to **write to** and **download from** BigQuery tables as PyArrow tables.


## Installation 

```bash 
pip install pyarrow-bigquery
```

## Quick Start

This guide will help you quickly get started with `pyarrow-bigquery`, a library that allows you to **read** from and **write** to Google BigQuery using PyArrow.

### Reading

`pyarrow-bigquery` exposes two methods to read BigQuery tables as PyArrow tables. Depending on your use case or the size of the table, you might want to use one method over the other.

**Read the Whole Table**

When the table is small enough to fit in memory, you can read it directly using `bq.read_table`.

```python
import pyarrow.bigquery as bq

table = bq.read_table("gcp_project.dataset.small_table")

print(table.num_rows)
```

**Read with Batches**

If the target table is larger than memory or you have other reasons not to fetch the whole table at once, you can use the `bq.reader` iterator method along with the `batch_size` parameter to limit how much data is fetched per iteration.

```python
import pyarrow.bigquery as bq

for table in bq.reader("gcp_project.dataset.big_table", batch_size=100):
    print(table.num_rows)
```

### Writing

Similarly, the package exposes two methods to write to BigQuery. Depending on your use case or the size of the table, you might want to use one method over the other.

**Write the Whole Table**

When you want to write a complete table at once, you can use the `bq.write_table` method.

```python
import pyarrow as pa
import pyarrow.bigquery as bq

table = pa.Table.from_arrays([[1, 2, 3, 4]], names=['integers'])

bq.write_table(table, 'gcp_project.dataset.table')
```

**Write in Batches (Smaller Chunks)**

If you need to write data in smaller chunks, you can use the `bq.writer` method with the `schema` parameter to define the table structure.

```python
import pyarrow as pa
import pyarrow.bigquery as bq

schema = pa.schema([
    ("integers", pa.int64())
])

with bq.writer("gcp_project.dataset.table", schema=schema) as w:
    w.write_batch(record_batch)
    w.write_table(table)
```

## API Reference

#### `pyarrow.bigquery.write_table`

Write a PyArrow Table to a BigQuery Table. No return value.

**Parameters:**

- `table`: `pa.Table`  
  PyArrow table.

- `where`: `str`  
  Destination location in BigQuery catalog.

- `project`: `str`, *default* `None`  
  BigQuery execution project, also the billing project. If not provided, it will be extracted from `where`.

- `table_create`: `bool`, *default* `True`  
  Specifies if the BigQuery table should be created.

- `table_expire`: `None | int`, *default* `None`  
  Amount of seconds after which the created table will expire. Used only if `table_create` is `True`. Set to `None` to disable expiration.

- `table_overwrite`: `bool`, *default* `False`  
  If the table already exists, destroy it and create a new one.

- `worker_type`: `threading.Thread | multiprocessing.Process`, *default* `threading.Thread`  
  Worker backend for fetching data.

- `worker_count`: `int`, *default* `os.cpu_count()`  
  Number of threads or processes to use for fetching data from BigQuery.

- `batch_size`: `int`, *default* `100`  
  Batch size for fetched rows.

```python
bq.write_table(table, 'gcp_project.dataset.table')
```

#### `pyarrow.bigquery.writer`

Context manager version of the write method. Useful when the PyArrow table is larger than memory size or the table is available in chunks.

**Parameters:**

- `schema`: `pa.Schema`  
  PyArrow schema.

- `where`: `str`  
  Destination location in BigQuery catalog.

- `project`: `str`, *default* `None`  
  BigQuery execution project, also the billing project. If not provided, it will be extracted from `where`.

- `table_create`: `bool`, *default* `True`  
  Specifies if the BigQuery table should be created.

- `table_expire`: `None | int`, *default* `None`  
  Amount of seconds after which the created table will expire. Used only if `table_create` is `True`. Set to `None` to disable expiration.

- `table_overwrite`: `bool`, *default* `False`  
  If the table already exists, destroy it and create a new one.

- `worker_type`: `threading.Thread | multiprocessing.Process`, *default* `threading.Thread`  
  Worker backend for writing data.

- `worker_count`: `int`, *default* `os.cpu_count()`  
  Number of threads or processes to use for writing data to BigQuery.

- `batch_size`: `int`, *default* `100`  
  Batch size used for writes. Table will be automatically split to this value.

Depending on the use case, you might want to use one of the methods below to write your data to a BigQuery table, using either `pa.Table` or `pa.RecordBatch`.

#### `pyarrow.bigquery.writer.write_table`

Context manager method to write a table.

**Parameters:**

- `table`: `pa.Table`  
  PyArrow table.

```python
import pyarrow as pa
import pyarrow.bigquery as bq

schema = pa.schema([("value", pa.list_(pa.int64()))])

with bq.writer("gcp_project.dataset.table", schema=schema) as w:
    for a in range(1000):
        w.write_table(pa.Table.from_pylist([{'value': [a] * 10}]))
```

#### `pyarrow.bigquery.writer.write_batch`

Context manager method to write a record batch.

**Parameters:**

- `batch`: `pa.RecordBatch`  
  PyArrow record batch.

```python
import pyarrow as pa
import pyarrow.bigquery as bq

schema = pa.schema([("value", pa.list_(pa.int64()))])

with bq.writer("gcp_project.dataset.table", schema=schema) as w:
    for a in range(1000):
        w.write_batch(pa.RecordBatch.from_pylist([{'value': [1] * 10}]))
```

#### `pyarrow.bigquery.read_table`

**Parameters:**

- `source`: `str`  
  BigQuery table location.

- `project`: `str`, *default* `None`  
  BigQuery execution project, also the billing project. If not provided, it will be extracted from `source`.

- `columns`: `str`, *default* `None`  
  Columns to download. When not provided, all available columns will be downloaded.

- `row_restrictions`: `str`, *default* `None`  
  Row level filtering executed on the BigQuery side. More in [BigQuery documentation](https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1beta1).

- `worker_type`: `threading.Thread | multiprocessing.Process`, *default* `threading.Thread`  
  Worker backend for fetching data.

- `worker_count`: `int`, *default* `os.cpu_count()`  
  Number of threads or processes to use for fetching data from BigQuery.

- `batch_size`: `int`, *default* `100`  
  Batch size used for fetching. Table will be automatically split to this value.

#### `pyarrow.bigquery.reader`

**Parameters:**

- `source`: `str`  
  BigQuery table location.

- `project`: `str`, *default* `None`  
  BigQuery execution project, also the billing project. If not provided, it will be extracted from `source`.

- `columns`: `str`, *default* `None`  
  Columns to download. When not provided, all available columns will be downloaded.

- `row_restrictions`: `str`, *default* `None`  
  Row level filtering executed on the BigQuery side. More in [BigQuery documentation](https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1beta1).

- `worker_type`: `threading.Thread | multiprocessing.Process`, *default* `threading.Thread`  
  Worker backend for fetching data.

- `worker_count`: `int`, *default* `os.cpu_count()`  
  Number of threads or processes to use for fetching data from BigQuery.

- `batch_size`: `int`, *default* `100`  
  Batch size used for fetching. Table will be automatically split to this value.

```python
import pyarrow as pa
import pyarrow.bigquery as bq

parts = []
for part in bq.reader("gcp_project.dataset.table"):
    parts.append(part)

table = pa.concat_tables(parts)
```

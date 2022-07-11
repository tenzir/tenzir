---
jupyter:
  jupytext:
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.3'
      jupytext_version: 1.14.0
  kernelspec:
    display_name: Python 3 (ipykernel)
    language: python
    name: python3
---

# Playing with VAST

Before running this, you need to execute `1-start.ipynb` and leave it in a running
state so that the VAST server stays up.

First, let us connect to the VAST instance:

```python
import asyncio
import pyvast
import requests
import logging
import pyarrow
import pandas

logging.getLogger().setLevel(logging.INFO)

vast = pyvast.VAST(binary="bin/vast", logger=logging.getLogger())
print("Connection successfull?", await vast.test_connection())
```

Then we import a few events:

```python
# Download test data
resp = requests.get('https://raw.githubusercontent.com/tenzir/vast/master/vast/integration/data/suricata/eve.json')
resp.raise_for_status()
resp_content = resp.content.decode()

# VAST import
proc = await vast.import_().suricata().exec(stdin=resp_content)
_, stderr = await proc.communicate()
print(stderr.decode())
```

Now let's check what made its way into the database by issuing a `count` query:

```python
proc = await vast.count().exec()
stdout, _ = await proc.communicate()
print(stdout.decode().strip())
```

We can export these events to Arrow tables:

```python
proc = await vast.export().arrow().exec()
stdout, _ = await proc.communicate()

istream = pyarrow.input_stream(io.BytesIO(stdout))
total_row_count = 0
tables = []

# An Arrow reader consumes a stream of batches with the same schema. When
# reading the result for a query that returns multiple schemas, VAST will use
# multiple writers. Hence, we try to open record batch readers until an
# exception occurs.
try:
    while True:
        print("open next reader")
        reader = pyarrow.ipc.RecordBatchStreamReader(istream)
        table = reader.read_all()
        total_row_count += table.num_rows
        tables.append(tables)
except pyarrow.ArrowInvalid:
    print(f"All {len(tables)} readers iterated, {total_row_count} records read")
```

These in turn can be converted into `pandas` dataframes:

```python
tables[0].to_pandas()
```

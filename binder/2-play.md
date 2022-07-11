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

We can export these events to `pandas` directly through Arrow:

```python
proc = await vast.export().arrow().exec()
stdout, _ = await proc.communicate()

rec_batch_reader = pyarrow.ipc.open_stream(stdout)
arrow_table = rec_batch_reader.read_all()
arrow_table.to_pandas()
```

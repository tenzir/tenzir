# Ingest

*Ingesting* refers to the import pipeline that covers the following steps:

```mermaid
flowchart LR
  classDef client fill:#00a4f1,stroke:none,color:#eee
  classDef server fill:#bdcfdb,stroke:none,color:#222

  load(Load):::client
  parse(Parse):::client
  ship(Ship):::client
  route(Route):::server
  index(Index):::server
  store(Store):::server

  load --> parse --> ship --> route --> index --> store
```

Blue stages happen at a client node and grey stages at the server node. The
following discussion assumes you have already [setup a VAST server
node](/docs/setup-vast).

:::note Lakehouse Architecture
VAST uses open standards for data in motion ([Arrow](https://arrow.apache.org))
and data at rest ([Parquet](https://parquet.apache.org/)). What VAST adds is an
intermediate layer of metadata management and indexing that jump-starts a
variety of use cases specific to security operations. In that sense, VAST
resembles a [lakehouse architecture][lakehouse-paper]. Crucially, you only ETL
data once to a destination of your choice. Think of the above pipeline as a
chain of independently operating microservices, each of which can be scaled
independently. The [actor
model](/docs/understand-vast/architecture/actor-model/) architecture of
VAST enables this naturally.
[lakehouse-paper]: http://www.cidrdb.org/cidr2021/papers/cidr2021_paper17.pdf
:::

## Understand the ingestion model

:::note TODO
Coming soon!
:::

## Load input from a carrier

A *carrier* is responsible for loading data from a dedicated I/O path.

:::note Refactoring Underway
We are currently reworking the data acquisition path by separating the I/O-bound
loading phase from the subsequent CPU-bound parsing phase. To date, these stages
are intertwined and take place together. In the future, the loading phase
performs block-level I/O and feeds data blocks to downstream parsers, each of
which responsible for decoding a specfic format.
:::

To date, the carrier is integrated into a specific format implementation, which
we discus next.

## Choose an input format

The *format* defines the encoding of data. ASCII formats include JSON, CSV, or
tool-specific data encodings like Zeek TSV. Examples for binary formats are
PCAP and NetFlow.

Choose an input format by passing it to `vast import` as sub-command, e.g., to
import a file in NDJSON, run:

```bash
vast import json < data.json
```

To see a list of available formats, run `vast import help`.

### JSON

:::note TODO
Coming soon!
:::
### CSV

:::note TODO
Coming soon!
:::
### Zeek

:::note TODO
Coming soon!
:::
### Suricata

:::note TODO
Coming soon!
:::
### NetFlow

:::note TODO
Coming soon!
:::
### PCAP

:::note TODO
Coming soon!
:::
### Argus

:::note TODO
Coming soon!
:::
## Provide a schema for unknown types

:::note TODO
Coming soon!
:::

## Organize types into modules
:::note TODO
Coming soon!
:::

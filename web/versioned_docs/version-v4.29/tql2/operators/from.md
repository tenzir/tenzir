---
sidebar_class_name: hidden
---

# from

Obtains events from an URI, inferring the source, compression and format.

```tql
from uri:string, [loader_args… { … }]
from events…
```

:::tip Use `from` if you can
The `from` operator is designed as an easy way to get data into Tenzir,
without having to manually write the separate steps of data ingestion manually.
:::

## Description

The `from` operator is an easy way to get data into Tenzir.
It will try to infer the connector, compression and format based on the given URI.

Alternatively, it can be used to create events from records.

### `uri: string`

The URI to load from.

### `loader_args… (optional)`

An optional set of arguments passed to the loader.
This can be used to e.g. pass credentials to a connector:

```tql
from "https://example.org/file.json", headers={Token: "XYZ"}
```

### `{ … } (optional)`

The optional pipeline argument allows for explicitly specifying how `from`
decompresses and parses data. By default, the pipeline is inferred based on a set
of [rules](#explanation).

If inference is not possible, or not sufficient, this argument can be used to
control the decompression and parsing. Providing this pipeline disables the inference. [Examples](#load-a-file-with-parser-arguments)

### `events…`

Instead of a URI, you can also provide one or more records, which will be
the operators output. This is mostly useful for testing pipelines without loading
actual data.

## Explanation

Loading a resource into tenzir consists of three steps:

* [**Loading**](#loading) the raw bytes
* [**Decompressing**](#decompressing) (optional)
* [**Reading**](#reading) the bytes as structured data

The `from` operator tries to infer all three steps from the given URI.

### Loading

The connector is inferred based on the URI `scheme://`. See the [URI schemes section](#uri-schemes) for supported
schemes. If no scheme is present, the connector attempts to load from the filesystem.

### Decompressing

The compression is inferred from the "file-ending" in the URI. Under the hood,
this uses the [`decompress_*` operators](../operators.md#encode--decode).
Supported compressions can be found in the [list of compression extensions](#compression).

The decompression step is optional and will only happen if a compression could be
inferred. If you know that the source is compressed and the compression cannot be
inferred, you can use the [pipeline argument](#---optional) to specify the
decompression manually.

### Reading

The format to read is, just as the compression, inferred from the file-ending.
Supported file formats are the common file endings for our [`read_*` operators](operators.md#parsing).

If you want to provide additional arguments to the parser, you can use the
[pipeline argument](#---optional) to specify the parsing manually. This can be useful,
if you e.g. know that the input is `suricata` or `ndjson` instead of just plain `json`.

### The pipeline argument & its relation to the loader

Some loaders, such as the [`load_tcp` operator](load_tcp.md), accept a sub-pipeline
directly. If the selected loader accepts a sub-pipeline, the `from` operator will
dispatch decompression and parsing into that sub-pipeline. If a an explicit
pipeline argument is provided it is forwarded as-is. If the loader does not accept
a sub-pipeline, the decompression and parsing steps are simply performed as part
of the regular pipeline.

#### Example transformation:

```tql title="from operator"
from "myfile.json.gz"
```
```tql title="Effective pipeline"
load_file "myfile.json.gz"
decompress_gzip
read_json
```

#### Example with pipeline argument:

```tql title="from operator"
from "tcp://0.0.0.0:12345", parallel=10 {
  read_gelf
}
```
```tql title="Effective pipeline"
load_tcp "tcp://0.0.0.0:12345", parallel=10 {
  read_gelf
}
```

## Supported Deductions

### URI schemes

| Scheme | Operator | Example |
|:------ |:-------- |:------- |
| `abfs`,`abfss` | [`load_azure_blob_storage`](load_azure_blob_storage.md) | `from "abfs://path/to/file.json"` |
| `amqp` | [`load_amqp`](load_amqp.md) | `from "amqp://…` |
| `file` | [`load_file`](load_file.md) | `from "file://path/to/file.json"` |
| `fluent-bit` | [`from_fluent_bit`](from_fluent_bit.mdx) | `from "fluent-bit://elasticsearch"` |
| `ftp`, `ftps` | [`load_ftp`](load_ftp.md) | `from "ftp://example.com/file.json"` |
| `gcps` | [`load_google_cloud_pubsub`](load_google_cloud_pubsub.md) | `from "gcps://project_id/subscription_id" { … }` |
| `http`, `https` | [`load_http`](load_http.md) | `from "http://example.com/file.json"` |
| `inproc` | [`load_zmq`](load_zmq.md) | `from "inproc://127.0.0.1:56789" { read_json }` |
| `kafka` | [`load_kafka`](load_kafka.md) | `from "kafka://topic" { read_json }` |
| `s3` | [`load_s3`](load_s3.md) | `from "s3://bucket/file.json"` |
| `sqs` | [`load_sqs`](load_sqs.md) | `from "sqs://my-queue" { read_json }` |
| `tcp` | [`load_tcp`](load_tcp.md) | `from "tcp://127.0.0.1:13245" { read_json }` |
| `udp` | [`load_udp`](load_udp.md) | `from "udp://127.0.0.1:56789" { read_json }` |
| `zmq` | [`load_zmq`](load_zmq.md) | `from "zmq://127.0.0.1:56789" { read_json }` |

Please see the respective operator pages for details on the URI's locator format.

### File extensions

#### Format

The `from` operator can deduce the file format based on these file-endings:

| Format | File Endings | Operator  |
|:------ |:------------ |:--------- |
|  CSV  | `.csv` | [`read_csv`](read_csv.mdx) |
|  Feather  | `.feather`, `.arrow` | [`read_feather`](read_feather.md) |
|  JSON  | `.json` | [`read_json`](read_json.mdx) |
|  NDJSON  | `.ndjson`, `.jsonl` | [`read_ndjson`](read_ndjson.mdx) |
|  Parquet  | `.parquet` | [`read_parquet`](read_parquet.md) |
|  Pcap  | `.pcap` | [`read_pcap`](read_pcap.md) |
|  SSV  | `.ssv` | [`read_ssv`](read_ssv.mdx) |
|  TSV  | `.tsv` | [`read_tsv`](read_tsv.mdx) |
|  YAML  | `.yaml` | [`read_yaml`](read_yaml.mdx) |

#### Compression

The `from` operator can deduce the following compressions based on these
file-endings:

| Compression |    File Endings  |
|:----------- |:---------------- |
| Brotli      | `.br`, `.brotli` |
| Bzip2       | `.bz2`           |
| Gzip        | `.gz`, `.gzip`   |
| LZ4         | `.lz4`           |
| Zstd        | `.zst`, `.zstd`  |

## Examples

### Load a local file

```tql
from "path/to/my/load/file.csv"
```

### Load a compressed file

```tql
from "path/to/my/load/file.json.bz2"
```

### Load a file with parser arguments

Provide an explicit header to the CSV parser:

```tql
from "path/to/my/load/file.csv.bz2" {
  decompress_brotli // this is now necessary due to the pipeline argument
  read_csv header="col1,col2,col3"
}
```

### Pick a more suitable parser

The file `eve.json` contains Suricata logs, but the `from` operator does not
know this. We provide an explicit `read_suricata` instead:

```tql
from "path/to/my/load/eve.json" {
  read_suricata
}
```

### Load from HTTP with a header

```tql
from "https://example.org/file.json", headers={Token: "1234"}
```
### Create events from records

```tql
from {message: "Value", endpoint: {ip: 127.0.0.1, port: 42}},
     {message: "Value", endpoint: {ip: 127.0.0.1, port: 42}, raw: "text"},
     {message: "Value", endpoint: null}
```
```tql
{
  message: "Value",
  endpoint: {
    ip: 127.0.0.1,
    port: 42
  }
}
{
  message: "Value",
  endpoint: {
    ip: 127.0.0.1,
    port: 42
  },
  raw: "text"
}
{
  message: "Value",
  endpoint: null
}
```

## See Also

[`to`](to.md)

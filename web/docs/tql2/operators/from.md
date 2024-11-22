---
sidebar_class_name: hidden
---

# from

Loads from an URI, inferring the source, compression and format.

```tql
from uri:str, [loader_args… { … }]
from event:record
from [ event… ]
```

:::tip Use `from` if you can
The `from` operator is designed as an easy way to get data into Tenzir,
without having to manually write the separate steps of data ingestion manually.
:::

## Description

The `from` operator is an easy way to get data into Tenzir.
It will try to infer the connector, compression and format based on the given URI.

Alternatively, it can be used to create ad-hoc events from records.

### `uri: str`

The URI to load from.

### `loader_args… (optional)`

An optional set of arguments passed to the loader.
This can be used to e.g. pass credentials to a connector:

```tql
from "https:://example.org/file.json", header={Token: 0}
```

### `{ … } (optional)`

A pipeline that can be used if inference for the compression or format does not work
or is not sufficient.

Providing this pipeline
disables the inference for the decompression and parsing format in order to avoid
confusion.

:::tip Only specify `{ … }` if you need it for data ingestion
The `{ … }` argument exists for data ingestion purposes. In most situations,
inference should be sufficient and the pipeline should not be required.

If you want to perform other operations on the data afterwards, continue the
pipeline after this operator instead of providing a sub-pipeline.
:::

### `event`/`events… :record`

Instead of a URI, you can also provide a *record' or a *list of records* as a
first argument to `from`. This will be treated as input events and simply passed
on as-is to the pipeline. This is mainly useful to test pipelines during
development.

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
this uses the [`decompress` operator](decompress.md).
Supported compressions can be found in the [list of supported codecs](decompress.md#codec-str).

The decompression step is optional and will only happen if a compression could be inferred.
If you know that the source is compressed and the compression cannot be inferred, you can use the
[pipeline argument](#---optional) to specify the decompression manually.

### Reading

The format to read is, just as the compression, inferred from the file-ending.
Supported file formats are the common file endings for our [`read_*` operators](operators.md#parsing).

If you want to provide additional arguments to the parser, you can use the
[pipeline argument](#---optional) to specify the parsing manually. This can be useful,
if you e.g. know that the input is `suricata` or `ndjson` instead of just plain `json`.

### The pipeline argument & its relation to the loader

Some loaders, such as the [`load_tcp` operator](load_tcp.md) accept a sub-pipeline
directly. If the selected loader accepts a sub-pipeline, the `from` operator will dispatch
decompression and parsing into that sub-pipeline. If a an explicit pipeline argument is provided
it is forwarded as-is. If the loader does not accept a sub-pipeline, the decompression and parsing
steps are simply performed as part of the regular pipeline.

#### Example transformation:

```tql title="from operator"
from "myfile.json.gz"
```
```tql title="Effective pipeline"
load_file "myfile.json.gz"
decompress "gzip"
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
| `file` | [`load_file`](load_file.md) | `from "file://path/to/file.json"` |
| `fluentbit` | [`from_fluent_bit`](from_fluent_bit.md) | `from "fluentbit://elasticsearch"` |
| `ftp`, `ftps` | [`load_ftp`](load_ftp.md) | `from "ftp://example.com/file.json"` |
| `gcps` | [`load_google_cloud_pubsub`](load_google_cloud_pubsub.md) | `from "gcps://project_id/subscription_id"` |
| `http`, `https` | [`load_http`](load_http.md) | `from "http://example.com/file.json"` |
| `s3` | [`load_s3`](load_s3.md) | `from "s3://bucket/file.json"` |
| `tcp` | [`load_tcp`](load_tcp.md) | `from "tcp://127.0.0.1:13245" { read_json }` |

Please see the respective operator pages for details on the URI's locator format.

### File endings

#### Format

The `from` operator can deduce the file format based on these file-endings:

| Format | File Endings | Operator  |
|:------ |:------------ |:--------- |
|  JSON  | `.json` | [`read_json`](read_json.md) |
|  NDJSON  | `.ndjson`,`.jsonl` | [`read_ndjson`](read_ndjson.md) |
|  YAML  | `.yaml` | [`read_yaml`](read_yaml.md) |
|  CSV  | `.csv` | [`read_csv`](read_csv.md) |
|  SSV  | `.ssv` | [`read_ssv`](read_ssv.md) |
|  TSV  | `.tsv` | [`read_tsv`](read_tsv.md) |

#### Compression

The `from` operator can deduce the following compressions based on these
file-endings:

| Compression |    File Endings   |
|:----------- |:----------------- |
| Brotli     | `.br`, `.brotli` |
| Bzip2       | `.bz2`            |
| Gzip        | `.gz`, `.gzip`    |
| LZ4         | `.lz4`            |
| Zstd        | `.zst`, `.zstd`   |

## Examples

### Load a local file

```tql
from "path/to/my/load/file.csv"
```

### Load a compressed file

```tql
from "path/to/my/load/file.json.bt"
```

### Load a file with parser arguments

Note how the explicit `decompress` step is now necessary:

```tql
from "path/to/my/load/file.json.bt" {
  decompress "brotoli" // this is now necessary due to the pipeline argument
  read_ndjson selector="event_type:suricata"
}
```

### Load from HTTP with a header

```tql
from "https:://example.org/file.json", header={Token: 0}
```
### Create a single event ad-hoc

```tql
from {message: "Value", endpoint: {ip: 127.0.0.1, port: 42}}
```
```tql
{
  message: "Value",
  endpoint: {
    ip: 127.0.0.1,
    port: 42
  }
}
```

### Create multiple events ad-hoc

```tql
from [
  {Field: "Value", Other_Field: 0 },
  {Field: "Value", Other_Field: 1 },
  {Field: "Value", Other_Field: 2 }
]
```
```tql
{
  Field: "Value",
  Other_Field: 0
}
{
  Field: "Value",
  Other_Field: 1
}
{
  Field: "Value",
  Other_Field: 2
}
```

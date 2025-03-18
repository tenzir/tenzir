---
sidebar_class_name: hidden
---

# to

Saves to an URI, inferring the destination, compression and format.

```tql
to uri:string, [saver_args… { … }]
```

:::tip Use `to` if you can
The `to` operator is designed as an easy way to get data out of Tenzir,
without having to manually write the separate steps of data formatting,
compression and writing.
:::

## Description

The `to` operator is an easy way to get data out of Tenzir into
It will try to infer the connector, compression and format based on the given URI.

### `uri: string`

The URI to load from.

### `saver_args… (optional)`

An optional set of arguments passed to the saver.
This can be used to e.g. pass credentials to a connector:

```tql
to "https://example.org/file.json", headers={Token: "XYZ"}
```

### `{ … } (optional)`

The optional pipeline argument allows for explicitly specifying how `to`
compresses and writes data. By default, the pipeline is inferred based on a set
of [rules](#explanation).

If inference is not possible, or not sufficient, this argument can be used to
control compression and writing. Providing this pipeline disables the inference.

## Explanation

Saving Tenzir data into some resource consists of three steps:

* [**Writing**](#writing) events as bytes according to some format
* [**compressing**](#compressing) (optional)
* [**Saving**](#saving) saving the bytes to some location

The `to` operator tries to infer all three steps from the given URI.

### Writing

The format to write inferred from the file-ending.
Supported file formats are the common file endings for our [`read_*` operators](operators.md#parsing).

If you want to provide additional arguments to the writer, you can use the
[pipeline argument](#---optional) to specify the parsing manually.

### Compressing

The compression, just as the format, is inferred from the "file-ending" in the URI. Under the hood,
this uses the [`decompress_*` operators](../operators.md#encode--decode).
Supported compressions can be found in the [list of compression extensions](#compression).

The compression step is optional and will only happen if a compression could be inferred.
If you want to write with specific compression settings, you can use the
[pipeline argument](#---optional) to specify the decompression manually.

### Saving

The connector is inferred based on the URI `scheme://`.
If no scheme is present, the connector attempts to save to the local filesystem.

## Supported Deductions

### URI schemes

| Scheme | Operator | Example |
|:------ |:-------- |:------- |
| `abfs`,`abfss` | [`save_azure_blob_storage`](save_azure_blob_storage.md) | `to "abfs://path/to/file.json"` |
| `amqp` | [`save_amqp`](save_amqp.md) | `to "amqp://…` |
| `elasticsearch` | [`to_opensearch`](to_opensearch.mdx) | `to "elasticsearch://…` |
| `file` | [`save_file`](save_file.md) | `to "file://path/to/file.json"` |
| `fluent-bit` | [`to_fluent_bit`](to_fluent_bit.mdx) | `to "fluent-bit://elasticsearch"` |
| `ftp`, `ftps` | [`save_ftp`](save_ftp.mdx) | `to "ftp://example.com/file.json"` |
| `gcps` | [`save_google_cloud_pubsub`](save_google_cloud_pubsub.md) | `to "gcps://project_id/topic_id" { … }` |
| `gs` | [`save_gcs`](save_gcs.md) | `to "gs://bucket/object.json"` |
| `http`, `https` | [`save_http`](save_http.mdx) | `to "http://example.com/file.json"` |
| `inproc` | [`save_zmq`](save_zmq.md) | `to "inproc://127.0.0.1:56789" { write_json }` |
| `kafka` | [`save_kafka`](save_kafka.md) | `to "kafka://topic" { write_json }` |
| `opensearch` | [`to_opensearch`](to_opensearch.mdx) | `to "opensearch://…` |
| `s3` | [`save_s3`](save_s3.md) | `to "s3://bucket/file.json"` |
| `sqs` | [`save_sqs`](save_sqs.md) | `to "sqs://my-queue" { write_json }` |
| `tcp` | [`save_tcp`](save_tcp.mdx) | `to "tcp://127.0.0.1:56789" { write_json }` |
| `udp` | [`save_udp`](save_udp.md) | `to "udp://127.0.0.1:56789" { write_json }` |
| `zmq` | [`save_zmq`](save_zmq.md) | `to "zmq://127.0.0.1:56789" { write_json }` |

Please see the respective operator pages for details on the URI's locator format.

### File extensions

#### Format

The `from` operator can deduce the file format based on these file-endings:

| Format | File Endings | Operator  |
|:------ |:------------ |:--------- |
|  CSV  | `.csv` | [`write_csv`](write_csv.md) |
|  Feather  | `.feather`, `.arrow` | [`write_feather`](write_feather.md) |
|  JSON  | `.json` | [`write_json`](write_json.md) |
|  NDJSON  | `.ndjson`, `.jsonl` | [`write_ndjson`](write_ndjson.md) |
|  Parquet  | `.parquet` | [`write_parquet`](write_parquet.md) |
|  Pcap  | `.pcap` | [`write_pcap`](write_pcap.md) |
|  SSV  | `.ssv` | [`write_ssv`](write_ssv.md) |
|  TSV  | `.tsv` | [`write_tsv`](write_tsv.md) |
|  YAML  | `.yaml` | [`write_yaml`](write_yaml.md) |

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

#### Example transformation:

```tql title="to operator"
to "myfile.json.gz"
```
```tql title="Effective pipeline"
write_json
compress_gzip
save_file "myfile.json.gz"
```

## Examples

### Save to a local file

```tql
to "path/to/my/output.csv"
```

### Save to a compressed file

```tql
to "path/to/my/output.csv.bz2"
```

## See Also

[from](from.md)

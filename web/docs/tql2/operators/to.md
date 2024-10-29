---
sidebar_class_name: hidden
---

# to

Saves to an URI, inferring the destination, compression and format.

```tql
to uri:str, [saver_args... { … }]
```

:::tip Use `to` if you can
The `to` operator is designed as an easy way to get data out of Tenzir,
without having to manually write the separate steps of data formatting,
compression and writing.
:::

## Description

The `to` operator is an easy way to get data out of Tenzir into
It will try to infer the connector, compression and format based on the given URI.

### `uri: str`

The URI to load from.

### `saver_args... (optional)`

An optional set of arguments passed to the saver.
This can be used to e.g. pass credentials to a connector:

```tql
to  "https:://example.org/file.json", header={Token: 0}
```

### `{ … } (optional)`

A pipeline that can be used if inference for the compression or format does not work
or is not sufficient.

Providing this pipeline
disables the inference for the decompression and writing format in order to avoid
confusion.

:::tip Only specify `{ … }` if you need it for data writing
The `{ … }` argument exists for data writing purposes. In most situations,
inference should be sufficient and the pipeline should not be required.

If you want to perform other operations on the data before saving, you should do that
as part of the pipeline before the `to` operator.
:::

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
[pipeline argument](#---optional) to specify the parsing manually. This can be useful,
if you want to write as `ndjson` instead of just plain `json`.

### Compressing

The compression, just as the format, is inferred from the "file-ending" in the URI. Under the hood,
this uses the [`decompress` operator](decompress.md).
Supported compressions can be found in the [list of supported codecs](decompress.md#codec-str).

The compression step is optional and will only happen if a compression could be inferred.
If you want to write with specific compression settings, you can use the
[pipeline argument](#---optional) to specify the decompression manually.

### Saving

The connector is inferred based on the URI `scheme://`.
If no scheme is present, the connector attempts to save to the local filesystem.

#### Example transformation:

```tql title="to operator"
to "myfile.json.gz"
```
```tql title="Effective pipeline"
write_json
compress "gzip"
save_file "myfile.json.gz"
```

#### Example with pipeline argument:

```tql title="to operator"
to "abfss://tenzirdev@demo/example.csv"
```
```tql title="Effective pipeline"
write_csv
save_azure_blob_storage "abfss://tenzirdev@demo/example.csv"
```

## Examples

### Save to a local file

```tql
to "path/to/my/output.csv"
```

### Save to a compressed file

```tql
to "path/to/my/output.csv.bt"
```

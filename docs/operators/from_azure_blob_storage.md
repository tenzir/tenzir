---
title: from_azure_blob_storage
category: Inputs/Events
example: 'from_azure_blob_storage "abfs://container/data/**.json"'
---

Reads one or multiple files from Azure Blob Storage.

```tql
from_azure_blob_storage url:string, [account_key=string, watch=bool,
  remove=bool, rename=string->string, path_field=field, max_age=duration] { … }
```

## Description

The `from_azure_blob_storage` operator reads files from Azure Blob Storage, with
support for glob patterns, automatic format detection, and file monitoring.

By default, authentication is handled by the Azure SDK's credential chain which
may read from multiple environment variables, such as:

- `AZURE_TENANT_ID`
- `AZURE_CLIENT_ID`
- `AZURE_CLIENT_SECRET`
- `AZURE_AUTHORITY_HOST`
- `AZURE_CLIENT_CERTIFICATE_PATH`
- `AZURE_FEDERATED_TOKEN_FILE`

### `url: string`

URL identifying the Azure Blob Storage location where data should be read from.

The characters `*` and `**` have a special meaning. `*` matches everything
except `/`. `**` matches everything including `/`. The sequence `/**/` can also
match nothing. For example, `container/**/data` matches `container/data`.

Supported URI formats:

1. `abfs[s]://<account>.blob.core.windows.net[/<container>[/<path>]]`
2. `abfs[s]://<container>@<account>.dfs.core.windows.net[/<path>]`
3. `abfs[s]://[<account>@]<host>[.<domain>][:<port>][/<container>[/<path>]]`
4. `abfs[s]://[<account>@]<container>[/<path>]`

(1) and (2) are compatible with the Azure Data Lake Storage Gen2 URIs, (3) is
for Azure Blob Storage compatible service including Azurite, and (4) is a
shorter version of (1) and (2).

:::tip[Authenticate with the Azure CLI]
Run `az login` on the command-line to authenticate the current user with Azure's
command-line arguments.
:::

### `account_key = string (optional)`

Account key for authenticating with Azure Blob Storage.

### `watch = bool (optional)`

In addition to processing all existing files, this option keeps the operator
running, watching for new files that also match the given URL. Currently, this
scans the filesystem up to every 10s.

Defaults to `false`.

### `remove = bool (optional)`

Deletes files after they have been read completely.

Defaults to `false`.

### `rename = string -> string (optional)`

Renames files after they have been read completely. The lambda function receives
the original path as an argument and must return the new path.

If the target path already exists, the operator will overwrite the file.

The operator automatically creates any intermediate directories required for the
target path. If the target path ends with a trailing slash (`/`), the original
filename will be automatically appended to create the final path.

### `path_field = field (optional)`

This makes the operator insert the path to the file where an event originated
from before emitting it.

By default, paths will not be inserted into the outgoing events.

### `max_age = duration (optional)`

Only process files that were modified within the specified duration from the
current time. Files older than this duration will be skipped.

### `{ … } (optional)`

Pipeline to use for parsing the file. By default, this pipeline is derived from
the path of the file, and will not only handle parsing but also decompression if
applicable.

## Examples

### Read every JSON file from a container

```tql
from_azure_blob_storage "abfs://my-container/data/**.json"
```

### Read CSV files using account key authentication

```tql
from_azure_blob_storage "abfs://container/data.csv", account_key="your-account-key"
```

### Read Suricata EVE JSON logs continuously

```tql
from_azure_blob_storage "abfs://logs/suricata/**.json", watch=true {
  read_suricata
}
```

### Process files and move them to an archive container

```tql
from_azure_blob_storage "abfs://input/**.json",
  rename=(path => "/archive/" + path)
```

### Add source path to events

```tql
from_azure_blob_storage "abfs://data/**.json", path_field=source_file
```

## See Also

[`from_file`](/reference/operators/from_file),
[`load_azure_blob_storage`](/reference/operators/load_azure_blob_storage),
[`save_azure_blob_storage`](/reference/operators/save_azure_blob_storage)

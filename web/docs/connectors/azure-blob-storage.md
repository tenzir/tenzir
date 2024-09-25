---
sidebar_custom_props:
  connector:
    loader: true
    saver: true
---

# azure-blob-storage

Loads from and saves to an Azure Blob Storage

## Synopsis

Loader:

```
azure-blob-storage <uri>
```

Saver:

```
azure-blob-storage <uri>
```

## Description

The `abs` loader connects to an Azure Blob Store to acquire raw bytes from a blob.
The `abs` saver writes bytes to a blob in an Azure Blob Store.

By default, authentication is handled by the Azure SDK’s credential chain which
may read from multiple environment variables, such as:

* `AZURE_TENANT_ID`
* `AZURE_CLIENT_ID`
* `AZURE_CLIENT_SECRET`
* `AZURE_AUTHORITY_HOST`
* `AZURE_CLIENT_CERTIFICATE_PATH`
* `AZURE_FEDERATED_TOKEN_FILE`

### `<uri>` (Loader, Saver)

A URI identifying the blob to load from or save to.

The saver will create paths and files if they do not exist and overwrite if
they do.

Supported URI formats:

1.     abfs[s]://[:<password>@]<account>.blob.core.windows.net [/<container>[/<path>]]

2.     abfs[s]://<container>[:<password>]@<account>.dfs.core.windows.net[/path]

3.     abfs[s]://[<account[:<password>]@]<host[.domain]>[<:port>] [/<container>[/path]]

4.     abfs[s]://[<account[:<password>]@]<container>[/path]

(1) and (2) are compatible with the Azure Data Lake Storage Gen2 URIs 1, (3) is
for Azure Blob Storage compatible service including Azurite, and (4) is a shorter
version of (1) and (2).

:::info
The credentials may be omitted, if the user running `tenzir` is logged in via the
Azure-CLI `az`
:::

## Examples

Read JSON from a blob `obj.json` in the blob container `container`, using the
`tenzirdev` user:

```
load azure-blob-storage "abfss://tenzirdev@container/obj.json"
| read json
```

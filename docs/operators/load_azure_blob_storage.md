---
title: load_azure_blob_storage
category: Inputs/Bytes
example: 'load_azure_blob_storage "abfs://container/file"'
---

Loads bytes from Azure Blob Storage.

```tql
load_azure_blob_storage uri:string, [account_key=string]
```

## Description

The `load_azure_blob_storage` operator loads bytes from an Azure Blob Storage.

By default, authentication is handled by the Azure SDK’s credential chain which
may read from multiple environment variables, such as:

- `AZURE_TENANT_ID`
- `AZURE_CLIENT_ID`
- `AZURE_CLIENT_SECRET`
- `AZURE_AUTHORITY_HOST`
- `AZURE_CLIENT_CERTIFICATE_PATH`
- `AZURE_FEDERATED_TOKEN_FILE`

### `uri: string`

A URI identifying the blob to load from.

Supported URI formats:

1. `abfs[s]://[:<password>@]<account>.blob.core.windows.net[/<container>[/<path>]]`
2. `abfs[s]://<container>[:<password>]@<account>.dfs.core.windows.net[/path]`
3. `abfs[s]://[<account[:<password>]@]<host[.domain]>[<:port>][/<container>[/path]]`
4. `abfs[s]://[<account[:<password>]@]<container>[/path]`

(1) and (2) are compatible with the Azure Data Lake Storage Gen2 URIs 1, (3) is
for Azure Blob Storage compatible service including Azurite, and (4) is a shorter
version of (1) and (2).

:::tip[Authenticate with the Azure CLI]
Run `az login` on the command-line to authenticate the current user with Azure's
command-line arguments.
:::

### `account_key = string (optional)`

Account key to authenticate with.

## Examples

### Write JSON

Read JSON from a blob `obj.json` in the blob container `container`, using the
`tenzirdev` user:

```tql
load_azure_blob_storage "abfss://tenzirdev@container/obj.json"
read_json
```

## See Also

[`save_azure_blob_storage`](/reference/operators/save_azure_blob_storage)

---
title: Entra workload identity federation for Azure Blob Storage
type: feature
authors:
  - lava
created: 2026-09-02T13:51:39.250699Z
---

`from_azure_blob_storage` and `to_azure_blob_storage` now take an `azure_auth`
record with a `web_identity` field, so a single node can read from and write to
blobs in several Microsoft Entra tenants at once:

```tql
from_azure_blob_storage "abfss://logs@customer1.dfs.core.windows.net/**.json.gz",
  azure_auth={
    tenant_id: secret("customer1-entra-tenant"),
    client_id: secret("customer1-entra-client"),
    web_identity: {
      token_endpoint: {
        url: env("ACTIONS_ID_TOKEN_REQUEST_URL"),
        query_params: {"audience": "api://AzureADTokenExchange"},
        headers: {
          "Authorization": "Bearer " + env("ACTIONS_ID_TOKEN_REQUEST_TOKEN"),
        },
        path: ".value",
      },
    },
  } {
  read_json
}
```

The OIDC token can come from a `token_endpoint`, a `token_file`, or an inline
`token`, which covers GitHub Actions, the GCP metadata server, and Kubernetes
projected service account tokens.

Previously workload identity federation was only reachable through the
process-global `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, and
`AZURE_FEDERATED_TOKEN_FILE` environment variables, which pinned the whole node
to one tenant. `azure_auth` also accepts `client_secret` instead of
`web_identity`, and cannot be combined with `account_key`.

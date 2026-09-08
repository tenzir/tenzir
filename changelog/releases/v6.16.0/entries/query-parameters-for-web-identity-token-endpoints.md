---
title: Query parameters for web identity token endpoints
type: feature
authors:
  - lava
created: 2026-09-02T00:00:00.000000Z
---

`web_identity.token_endpoint` now takes a `query_params` record, so the audience
an OIDC provider expects no longer has to be concatenated onto the URL by hand.

Keys and values are percent-encoded and joined onto whatever query the URL
already carries, which matters for GitHub Actions: its
`ACTIONS_ID_TOKEN_REQUEST_URL` already ends in an `api-version` parameter. Both
`string` and `secret` values are accepted, like `headers`.

```tql
from_azure_blob_storage "abfss://container@account.dfs.core.windows.net/blob.json",
  azure_auth={
    tenant_id: secret("entra-tenant-id"),
    client_id: secret("entra-client-id"),
    web_identity: {
      token_endpoint: {
        url: env("ACTIONS_ID_TOKEN_REQUEST_URL"),
        query_params: { "audience": "api://AzureADTokenExchange" },
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

Entra always expects `api://AzureADTokenExchange`, but the parameter is not
Azure-specific: AWS wants `sts.amazonaws.com` there, and other providers name
the parameter something else entirely.

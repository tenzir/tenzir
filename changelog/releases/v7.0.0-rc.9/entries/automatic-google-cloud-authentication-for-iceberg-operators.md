---
title: Automatic Google Cloud authentication for Iceberg operators
type: change
authors:
  - mavam
created: 2026-11-25T11:46:08.046159Z
---

`to_iceberg` no longer accepts the `gcp_auth` option. Google Cloud
authentication is now enabled automatically when `gcp_service_account_key` or
`gcp_project` is set, or when `catalog` points to an `https://*.googleapis.com`
endpoint (such as Google BigLake) without an explicit `token`.

In addition, `gs://` table locations automatically resolve through native GCS
storage unless explicit S3 settings (`aws_iam`, `catalog_aws_service`, or
`s3_endpoint`) are configured.

Before:

```tql
to_iceberg "showcase.events",
  catalog="https://biglake.googleapis.com/iceberg/v1/restcatalog",
  warehouse="gs://tenzir-demo-iceberg",
  gcp_auth=true,
  gcp_project="tenzir-demo"
```

After:

```tql
to_iceberg "showcase.events",
  catalog="https://biglake.googleapis.com/iceberg/v1/restcatalog",
  warehouse="gs://tenzir-demo-iceberg",
  gcp_project="tenzir-demo"
```

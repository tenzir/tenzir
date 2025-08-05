---
title: to_amazon_security_lake
category: Outputs/Events
example: 'to_amazon_security_lake "s3://…"'
---

Sends OCSF events to Amazon Security Lake.

```tql
to_amazon_security_lake s3_uri:string, region=string, account_id=string,
                       [timeout=duration, role=string, external_id=string]
```

## Description

The `to_amazon_security_lake` operator sends OCSF events to [Amazon Security
Lake](https://aws.amazon.com/security-lake/), AWS's centralized security data
repository that normalizes and stores security data from multiple sources.

The operator automatically handles Amazon Security Lake's partitioning
requirements and file size constraints, but does not validate the OCSF schema of
the events. Consider [`ocsf::apply`](/reference/operators/ocsf/apply) in your
pipeline to ensure schema compliance.

For a list of OCSF event classes supported by Amazon Security Lake, see the [AWS
documentation](https://docs.aws.amazon.com/security-lake/latest/userguide/adding-custom-sources.html#ocsf-eventclass).
The operator generates random UUID (v7) file names with a `.parquet` extension.

### `s3_uri: string`

The base URI for the S3 storage backing the lake in the form

```
s3://<bucket>/ext/<custom-source-name>
```

Replace the placeholders as follows:

- `<bucket>`: the bucket associated with your lake
- `<custom-source-name>`: the name of your custom Amazon Security Lake source

You can copy this URI directly from the AWS Security Lake custom source interface.

### `region = string`

The region for partitioning.

### `account_id = string`

The AWS account ID or external ID you chose when creating the Amazon Security Lake
custom source.

:::note
The user running the Tenzir Node must have permissions to write to the given
partition:

```
{s3_uri}/region={region}/accountId={account_id}/
```

:::

### `timeout = duration (optional)`

A duration after which the operator will write to Amazon Security Lake,
regardless of file size. Amazon Security Lake requires this to be between `5min`
and `1d`.

Defaults to `5min`.

### `role = string (optional)`

A role to assume when writing to S3.

When not specified, the operator automatically uses the standard Amazon Security
Lake provider role based on your configuration:
`arn:aws:iam::<account_id>:role/AmazonSecurityLake-Provider-<custom-source-name>-<region>`

The operator extracts the custom source name from the provided S3 URI.

For example, given:

- `account_id`: `"123456789012"`
- `s3_uri`: `"s3://aws-security-data-lake-…/ext/tnz-ocsf-4001/"`
- `region`: `"eu-west-1"`

The operator will use:
`arn:aws:iam::123456789012:role/AmazonSecurityLake-Provider-tnz-ocsf-4001-eu-west-1`

### `external_id = string (optional)`

The external ID to use when assuming the `role`.

Defaults to no ID.

## Examples

### Send OCSF Network Activity events to Amazon Security Lake

This example shows how to send OCSF Network Activity events to an AWS Security
Lake running on `eu-west-2` with a custom source called
`tenzir_network_activity` and account ID `123456789012`:

```tql
let $s3_uri = "s3://aws-security-data-lake-eu-west-2-lake-abcdefghijklmnopqrstuvwxyz1234/ext/tnz-ocsf-4001/"

subscribe "ocsf"
where @name == "ocsf.network_activity"
ocsf::apply
to_amazon_security_lake $s3_uri,
  region="eu-west-2",
  account_id="123456789012"
```

## See Also

[`ocsf::apply`](/reference/operators/ocsf/apply),
[`save_s3`](/reference/operators/save_s3)

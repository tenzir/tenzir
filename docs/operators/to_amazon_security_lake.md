---
title: to_amazon_security_lake
category: Outputs/Events
example: 'to_amazon_security_lake "s3://…"'
---

Sends events to Amazon Security Lake.

```tql
to_amazon_security_lake s3_uri:string, region=string, account_id=string,
                       [timeout=duration, role=string, external_id=string]
```

## Description

The `to_amazon_security_lake` operator sends OCSF events to an [Amazon Security Lake][asl].

The events **must** match the Lakes selected OCSF event class. The operator does not
perform any validation on the events.
OCSF event classes supported by ASL can be found [here](https://docs.aws.amazon.com/security-lake/latest/userguide/adding-custom-sources.html#ocsf-eventclass).

The operator takes care of ASL's partitioning and file size requirements.
The file names will be randomly generated UUIDs (v4) with a `.parquet` file
extension.

[asl]: https://aws.amazon.com/security-lake/

### `s3_uri: string`

The base URI for the S3 storage backing the lake in the form

```
s3://<bucket>/ext/<custom-source-name>
```

Replace the placeholders as follows:

* `<bucket>`: the bucket associated with your lake
* `<custom-source-name>`: the name of your custom ASL source

This URI can be directly copied from the AWS security lake custom source interface.

### `region = string`

The region for partitioning.

### `account_id = string`

The AWS accountID or external ID chosen when creating the ASL custom source.

:::note
Be aware that the user running the Tenzir Node must have permissions to write to
the given partition:
```
{s3_uri}/region={region}/accountId={account_id}/
```
:::

### `timeout = duration (optional)`

A duration after which the operator will write to ASL, regardless of file size.
ASL specifies this should be between `5min` and `1d`.

The default is `5min`.

### `role = string (optional)`

A role to assume when writing to S3.

### `external_id = string (optional)`

The external ID to use when assuming the `role`.

Defaults to no ID.

## Examples

### Send OCSF Network Activity events to ASL

Given a AWS security lake running on `eu-west-2`, a custom source called
`tenzir_network_activity` set up on that lake, and an account with id
`123456789012`
for it:

```tql
let $s3_uri = "s3://aws-security-data-lake-eu-west-2-lake-abcdefghijklmnopqrstuvwxyz1234/ext/tenzir_network_activity/"

export
where @name == "ocsf.network_activity"
to_amazon_security_lake $s3_uri,
  region="eu-west-2",
  accountId="123456789012"
```

## See Also

[`save_s3`](/reference/operators/save_s3)

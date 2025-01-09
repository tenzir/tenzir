# to_asl

Sends events to [Amazon Security Lake (ASL)][asl].

[asl]: https://aws.amazon.com/security-lake/

```tql
to_asl s3_uri:string, [region=string, account_id=string, timeout=duration]
```

## Description

The `to_asl` operator sends OCSF events to an [Amazon Security Lake][asl].

The events must match the Lakes selected OCSF event class. The operator does not
perform any validation on the events.
OCSF event classes supported by ASL can be found [here](https://docs.aws.amazon.com/security-lake/latest/userguide/adding-custom-sources.html#ocsf-eventclass).

The operator takes care of the final partitioning by date. The file names will
be 16 byte uuids with a `.parquet` extension.

### `s3_uri: string`

The base URI for the S3 storage backing the lake.

It must have one of the forms

```
s3://<bucket>/ext/<custom-source-name>/region=<region>/accountId=<account-id>
s3://<bucket>/ext/<custom-source-name>
```

* `<bucket>`: the bucket associated with your lake
* `<custom-source>`: the name of your custom ASL source
* `<region>`: the AWS region of your lake
* `<account-id>`: the account ID you use

The `s3://<bucket>/ext/<custom-source-name>` can be obtained from the ASL interface.

If the second form is chosen, the `region` and `account_id` parameters must be
specified, otherwise they must not be specified.

### `region = string`

The region for partitioning. This argument is required if the `region` is not
specified in the URI.

### `account_id = string`

The AWS accountID chosen when creating the ASL custom source. This argument is
required if the `region` is not specified in the URI.

### `timeout = duration (optional)`

A duration after which the operator will write to ASL, regardless of file size.
ASL specifies this should be between `5min` and `1day`.

The default is `5min`.

## Examples

### Send all stored OCSF Network Activity events to ASL

```tql
export
where @name == "ocsf.network_activity"
to_asl "s3://aws-security-data-lake-us-west-2-lake-uid/ext/tenzir_network_activity/region=us-west-2/accountId=123456789012"
```

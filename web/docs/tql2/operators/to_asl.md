# to_asl

Sends events to [Amazon Security Lake (ASL)][asl].

[asl]: https://aws.amazon.com/security-lake/

```tql
to_asl s3_uri:string, region=string, account_id=string, [timeout=duration]
```

## Description

The `to_asl` operator sends OCSF events to an [Amazon Security Lake][asl].

The events **must** match the Lakes selected OCSF event class. The operator does not
perform any validation on the events.
OCSF event classes supported by ASL can be found [here](https://docs.aws.amazon.com/security-lake/latest/userguide/adding-custom-sources.html#ocsf-eventclass).

The operator takes care of the Lakes partitioning and file size requirement.
The file names will be 16 byte uuids with a `.parquet` extension.

### `s3_uri: string`

The base URI for the S3 storage backing the lake.

It must have the form

```
s3://<bucket>/ext/<custom-source-name>
```

* `<bucket>`: the bucket associated with your lake
* `<custom-source-name>`: the name of your custom ASL source

This URI can be directly copied from the AWS security lake custom source interface.

### `region = string`

The region for partitioning.

### `account_id = string`

The AWS accountID chosen when creating the ASL custom source.

### `timeout = duration (optional)`

A duration after which the operator will write to ASL, regardless of file size.
ASL specifies this should be between `5min` and `1day`.

The default is `5min`.

## Examples

### Send all stored OCSF Network Activity events to ASL

Given a AWS security lake running on `eu-west-2`, a custom source called
`tenzir_network_activity` set up on that lake and an account with id `123456789012`
for it:

```tql
let $s3_uri = "s3://aws-security-data-lake-eu-west-2-lake-abcdefghijklmnopqrstuvwxzz1234/ext/tenzir_network_activity/"

export
where @name == "ocsf.network_activity"
to_asl §s3_uri,
  region="eu-west-2",
  accountId="123456789012"
```

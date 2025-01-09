# Security Lake

[Amazon Security Lake (ASL)][asl] is an OCSF event collection service.

[asl]: https://aws.amazon.com/security-lake/

![amazon-security-lake](amazon-security-lake.svg)

Tenzir can send events to ASL via the [`to_asl` operator](../../../tql2/operators/to_asl.md).

## Configuration

Follow the [standard configuration instructions](../README.md) to authenticate
with your AWS credentials.

Set up a custom source in ASL and use its s3 bucket URI with the `to_asl` operator.

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

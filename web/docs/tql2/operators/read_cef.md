# read_cef

Parses an incoming [Common Event Format (CEF)][cef] stream into events.

```tql
read_cef [merge=bool, raw=bool, schema=str, selector=str, schema_only=bool, unflatten=str]
```

## Description

The [Common Event Format (CEF)][cef] is a text-based event format that
originally stems from ArcSight. It is line-based and human readable. The first 7
fields of a CEF event are always the same, and the 8th *extension* field is an
optional list of key-value pairs:

[cef]: https://community.microfocus.com/cfs-file/__key/communityserver-wikis-components-files/00-00-00-00-23/3731.CommonEventFormatV25.pdf

```
CEF:Version|Device Vendor|Device Product|Device Version|Device Event Class ID|Name|Severity|[Extension]
```

Here is a real-world example:

```
CEF:0|Cynet|Cynet 360|4.5.4.22139|0|Memory Pattern - Cobalt Strike Beacon ReflectiveLoader|8| externalId=6 clientId=2251997 scanGroupId=3 scanGroupName=Manually Installed Agents sev=High duser=tikasrv01\\administrator cat=END-POINT Alert dhost=TikaSrv01 src=172.31.5.93 filePath=c:\\windows\\temp\\javac.exe fname=javac.exe rt=3/30/2022 10:55:34 AM fileHash=2BD1650A7AC9A92FD227B2AB8782696F744DD177D94E8983A19491BF6C1389FD rtUtc=Mar 30 2022 10:55:34.688 dtUtc=Mar 30 2022 10:55:32.458 hostLS=2022-03-30 10:55:34 GMT+00:00 osVer=Windows Server 2016 Datacenter x64 1607 epsVer=4.5.5.6845 confVer=637842168250000000 prUser=tikasrv01\\administrator pParams="C:\\Windows\\Temp\\javac.exe" sign=Not signed pct=2022-03-30 10:55:27.140, 2022-03-30 10:52:40.222, 2022-03-30 10:52:39.609 pFileHash=1F955612E7DB9BB037751A89DAE78DFAF03D7C1BCC62DF2EF019F6CFE6D1BBA7 pprUser=tikasrv01\\administrator ppParams=C:\\Windows\\Explorer.EXE pssdeep=49152:2nxldYuopV6ZhcUYehydN7A0Fnvf2+ecNyO8w0w8A7/eFwIAD8j3:Gxj/7hUgsww8a0OD8j3 pSign=Signed and has certificate info gpFileHash=CFC6A18FC8FE7447ECD491345A32F0F10208F114B70A0E9D1CD72F6070D5B36F gpprUser=tikasrv01\\administrator gpParams=C:\\Windows\\system32\\userinit.exe gpssdeep=384:YtOYTIcNkWE9GHAoGLcVB5QGaRW5SmgydKz3fvnJYunOTBbsMoMH3nxENoWlymW:YLTVNkzGgoG+5BSmUfvJMdsq3xYu gpSign=Signed actRem=Kill, Rename
```

The [CEF specification][cef] pre-defines several extension field key names and
data types for the corresponding values. Tenzir's parser does not enforce the
strict definitions and instead tries to infer the type from the provided values.

Tenzir translates the `extension` field to a nested record, where the key-value
pairs of the extensions map to record fields. Here is an example of the above
event:

```json
{
  "cef_version": 0,
  "device_vendor": "Cynet",
  "device_product": "Cynet 360",
  "device_version": "4.5.4.22139",
  "signature_id": "0",
  "name": "Memory Pattern - Cobalt Strike Beacon ReflectiveLoader",
  "severity": "8",
  "extension": {
    "externalId": 6,
    "clientId": 2251997,
    "scanGroupId": 3,
    "scanGroupName": "Manually Installed Agents",
    "sev": "High",
    "duser": "tikasrv01\\administrator",
    "cat": "END-POINT Alert",
    "dhost": "TikaSrv01",
    "src": "172.31.5.93",
    "filePath": "c:\\windows\\temp\\javac.exe",
    "fname": "javac.exe",
    "rt": "3/30/2022 10:55:34 AM",
    "fileHash": "2BD1650A7AC9A92FD227B2AB8782696F744DD177D94E8983A19491BF6C1389FD",
    "rtUtc": "Mar 30 2022 10:55:34.688",
    "dtUtc": "Mar 30 2022 10:55:32.458",
    "hostLS": "2022-03-30 10:55:34 GMT+00:00",
    "osVer": "Windows Server 2016 Datacenter x64 1607",
    "epsVer": "4.5.5.6845",
    "confVer": 637842168250000000,
    "prUser": "tikasrv01\\administrator",
    "pParams": "C:\\Windows\\Temp\\javac.exe",
    "sign": "Not signed",
    "pct": "2022-03-30 10:55:27.140, 2022-03-30 10:52:40.222, 2022-03-30 10:52:39.609",
    "pFileHash": "1F955612E7DB9BB037751A89DAE78DFAF03D7C1BCC62DF2EF019F6CFE6D1BBA7",
    "pprUser": "tikasrv01\\administrator",
    "ppParams": "C:\\Windows\\Explorer.EXE",
    "pssdeep": "49152:2nxldYuopV6ZhcUYehydN7A0Fnvf2+ecNyO8w0w8A7/eFwIAD8j3:Gxj/7hUgsww8a0OD8j3",
    "pSign": "Signed and has certificate info",
    "gpFileHash": "CFC6A18FC8FE7447ECD491345A32F0F10208F114B70A0E9D1CD72F6070D5B36F",
    "gpprUser": "tikasrv01\\administrator",
    "gpParams": "C:\\Windows\\system32\\userinit.exe",
    "gpssdeep": "384:YtOYTIcNkWE9GHAoGLcVB5QGaRW5SmgydKz3fvnJYunOTBbsMoMH3nxENoWlymW:YLTVNkzGgoG+5BSmUfvJMdsq3xYu",
    "gpSign": "Signed",
    "actRem": "Kill, Rename"
  }
}
```

### `merge=bool (optional)`

Merges all incoming events into a single schema\* that converges over time. This
option is usually the fastest *for reading* highly heterogeneous data, but can
lead
to huge schemas filled with nulls and imprecise results. Use with caution.

\*: In selector mode, only events with the same selector are merged.

This option can not be combined with `raw=true, schema=<schema>`.

### `raw=bool (optional)`

Use only the raw types that are native to the parsed format. Fields that have a
type
specified in the chosen schema will still be parsed according to the schema.

For example, the JSON format has no notion of an IP address, so this will cause
all IP addresses
to be parsed as strings, unless the field is specified to be an IP address by
the schema.
JSON however has numeric types, so those would be parsed.

Use with caution.

### `schema=str (optional)`

Provide the name of a [schema](../../data-model/schemas.md) to be used by the
parser. If the schema uses the `blob` type, then the Syslog parser expects
base64-encoded strings.

The `schema` option is incompatible with the `selector` option.

### `selector=str (optional)`

Designates a field value as schema name with an optional dot-separated prefix.

For example, the Suricata EVE JSON format includes a field
`event_type` that contains the event type. Setting the selector to
`event_type:suricata` causes an event with the value `flow` for the field
`event_type` to map onto the schema `suricata.flow`.

The `selector` option is incompatible with the `schema` option.

### `schema_only=bool (optional)`

When working with an existing schema, this option will ensure that the output
schema has *only* the fields from that schema. If the schema name is obtained
via a `selector`
and it does not exist, this has no effect.

This option requires either `schema` or `selector` to be set.

### `unflatten=str (optional)`

A delimiter that, if present in keys, causes values to be treated as values of
nested records.

A popular example of this is the [Zeek JSON](read_zeek_json.md) format. It includes
the fields `id.orig_h`, `id.orig_p`, `id.resp_h`, and `id.resp_p` at the
top-level. The data is best modeled as an `id` record with four nested fields
`orig_h`, `orig_p`, `resp_h`, and `resp_p`.

Without an unflatten separator, the data looks like this:

```json
{
  "id.orig_h" : "1.1.1.1",
  "id.orig_p" : 10,
  "id.resp_h" : "1.1.1.2",
  "id.resp_p" : 5
}
```

With the unflatten separator set to `.`, Tenzir reads the events like this:

```json
{
  "id" : {
    "orig_h" : "1.1.1.1",
    "orig_p" : 10,
    "resp_h" : "1.1.1.2",
    "resp_p" : 5
  }
}
```

## Examples

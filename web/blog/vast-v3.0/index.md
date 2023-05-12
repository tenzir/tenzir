---
title: VAST v3.0
description: VAST Language Evolution — Dataflow Pipelines
authors: [dominiklohmann, dakostu]
image: /img/blog/building-blocks.excalidraw.svg
date: 2023-03-14
tags: [release, pipelines, language, cef, performance, introspection, regex]
---

[VAST v3.0][github-vast-release] is out. This release brings some major updates
to the the VAST language, making it easy to write down dataflow pipelines that
filter, reshape, aggregate, and enrich security event data. Think of VAST as
security data pipelines plus open storage engine.

[github-vast-release]: https://github.com/tenzir/vast/releases/tag/v3.0.4

<!--truncate-->

![Pipelines and Storage](/img/blog/building-blocks.excalidraw.svg)

## The VAST Language: Dataflow Pipelines

Starting with v3.0, VAST introduces a new way to write pipelines, with a syntax
similar to [splunk](https://splunk.com), [Kusto][kusto],
[PRQL](https://prql-lang.org/), and [Zed](https://zed.brimdata.io/). Previously,
VAST only supported a YAML-like definition of pipelines in configuration files
to deploy them statically during import, export, or use them during compaction.

[kusto]: https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/

The new syntax resembles the well-known Unix paradigm of command chaining. The
difference to Unix pipelines is that VAST exchanges structured data between
operators. The `vast export` and `vast import` commands now accept such a
pipeline as a string argument. Refer to the [pipelines
documentation][pipeline-doc] for more details on how to use the new pipeline
syntax.

:::info Pipeline YAML Syntax Deprecation
This release introduces a transitional period from YAML-style to textual
pipelines. The old YAML syntax for pipelines will be deprecated and removed
altogether in a future version. The new pipeline operators [`head`][head-op] and
[`taste`][taste-op] have no YAML equivalent.
:::

[pipeline-doc]: /docs/VAST%20v3.0/understand/language/pipelines#define-a-pipeline
[head-op]: /docs/VAST%20v3.0/understand/language/operators/head
[taste-op]: /docs/VAST%20v3.0/understand/language/operators/taste

## Language Upgrades

We've made some breaking changes to the the VAST language that we've wanted to
do for a long time. Here's a summary:

1. We removed the term VASTQL: The VAST Query Language is now simply
   the VAST language, and "VAST" will supersede the "VASTQL" abbreviation.

2. Several built-in types have a new name:

   - `int` → `int64`
   - `count` → `uint64`
   - `real` → `double`
   - `addr` → `ip`

   The old names are still supported for the time being, but trigger a
   warning on startup. We will remove support for the old names in a future
   release.

3. The match operator `~` and its negated form `!~` no longer exist. Use `==`
   and `!=` instead to perform searches with regular expressions, e.g., `url ==
   /^https?.*/`. Such queries now work for all string fields in addition to the
   previously supported `#type` meta extractor.

4. We removed the `#field` meta extractor. That is, queries of the form `#field
   == "some.field.name"` no longer work. Use `some.field.name != null` or the
   new short form `some.field.name` to check for field existence moving forward.

5. We renamed the boolean literal values `T` and `F` to `true` and `false`,
   respectively. For example the query `suricata.alert.alerted == T` is no
   longer valid; use `suricata.alert.alerted == true` instead.

6. We renamed the non-value literal value `nil` to `null`. For example the
   query `x != nil` is no longer valid; use `x != null` instead.

7. The `map` type no longer exists: Instead of `map<T, U>`, use the equivalent
   `list<record{ key: T, value: U }>`.

Our goal is for these changes to make the query language feel more natural to
our users. We've got [big plans][rfc-001] on how to extend it—and this felt very
much necessary as a preparatory step to making the language more useful.

[rfc-001]: https://github.com/tenzir/vast/blob/main/rfc/001-composable-pipelines/README.md

## Regular Expression Evaluation

VAST now supports searching with regular expressions. For example, let's say you
are looking for all events that contain a GUID surrounded by braces whose third
and fourth section are `5ec2-7015`:

```json {0} title="vast export -n 1 json '/\{[0-9a-f]{8}-[0-9a-f]\{4}-5ec2-7015-[0-9a-f]\{12}\}/'"
{
  "RuleName": "-",
  "UtcTime": "2020-05-18T09:42:40.443000",
  "ProcessGuid": "{8bcf3217-5890-5ec2-7015-00000000b000}",
  "ProcessId": 172,
  "Image": "C:\\Windows\\System32\\conhost.exe",
  "FileVersion": "10.0.17763.1075 (WinBuild.160101.0800)",
  "Description": "Console Window Host",
  "Product": "Microsoft® Windows® Operating System",
  "Company": "Microsoft Corporation",
  "OriginalFileName": "CONHOST.EXE",
  "CommandLine": "\\??\\C:\\Windows\\system32\\conhost.exe 0xffffffff -ForceV1",
  "CurrentDirectory": "C:\\Windows",
  "User": "NT AUTHORITY\\SYSTEM",
  "LogonGuid": "{8bcf3217-54f5-5ebe-e703-000000000000}",
  "LogonId": 999,
  "TerminalSessionId": 0,
  "IntegrityLevel": "System",
  "Hashes": "SHA1=74F28DD9B0DA310D85F1931DB2749A26A9A8AB02",
  "ParentProcessGuid": "{8bcf3217-5890-5ec2-6f15-00000000b000}",
  "ParentProcessId": 3440,
  "ParentImage": "C:\\Windows\\System32\\OpenSSH\\sshd.exe",
  "ParentCommandLine": "\"C:\\Windows\\System32\\OpenSSH\\sshd.exe\" \"-R\""
}
```

:::tip Case-Insensitive Patterns
In addition to writing `/pattern/`, you can specify a regular expression that
ignores the casing of characters via `/pattern/i`. The `/i` flag is currently
the only support pattern modifier.
:::

## Revamped Status for Event Distribution

The event distribution statistics moved within the output of `vast status`.

They were previously available under the `index.statistics` section when using
the `--detailed` option:

```json {0} title="VAST v2.4.1 ❯ vast status --detailed | jq .index.statistics"
{
  "events": {
    "total": 42
  },
  "layouts": {
    "suricata.alert": {
      "count": 1,
      "percentage": 2.4
    },
    "suricata.flow": {
      "count": 41,
      "percentage": 97.6
    }
  }
}
```

It is now under the `catalog` section and shows some additional information:

```json {0} title="VAST v3.0 ❯ vast status | jq .catalog"
{
  "num-events": 42,
  "num-partitions": 3,
  "schemas": {
    "suricata.alert": {
      "import-time": {
        "max": "2023-01-13T22:51:23.730183",
        "min": "2023-01-13T22:51:23.730183"
      },
      "num-events": 1,
      "num-partitions": 1
    },
    "suricata.flow": {
      "import-time": {
        "max": "2023-01-13T22:51:24.127312",
        "min": "2023-01-13T23:13:01.991323"
      },
      "num-events": 41,
      "num-partitions": 2
    }
  }
}
```

## Display Schema of Stored Events

The `vast show schemas` command makes it easy to see the structure of events in
the database at a glance.

```yaml {0} title="vast show schemas --yaml suricata.flow"
- suricata.flow:
    record:
      - timestamp:
          timestamp: time
      - flow_id:
          type: uint64
          attributes:
            index: hash
      - pcap_cnt: uint64
      - vlan:
          list: uint64
      - in_iface: string
      - src_ip: ip
      - src_port:
          port: uint64
      - dest_ip: ip
      - dest_port:
          port: uint64
      - proto: string
      - event_type: string
      - community_id:
          type: string
          attributes:
            index: hash
      - flow:
          suricata.component.flow:
            record:
              - pkts_toserver: uint64
              - pkts_toclient: uint64
              - bytes_toserver: uint64
              - bytes_toclient: uint64
              - start: time
              - end: time
              - age: uint64
              - state: string
              - reason: string
              - alerted: bool
      - app_proto: string
```

:::tip Filter Schemas
The `vast show schemas` command supports filtering not just by the exact name of
a schema, but also by the module name. E.g., `vast show schemas zeek` will print
a list of all schemas in the Zeek module that the VAST server holds data for.
:::

## Common Event Format (CEF) Parser

This release includes a new reader plugin for the [Common Event Format
(CEF)][cef], a text-based event format that originally stems from ArcSight. This
line-based format consists of up to 8 pipe-separated fields, with the last field
being an optional list of key-value pairs:

[cef]: https://www.microfocus.com/documentation/arcsight/arcsight-smartconnectors/pdfdoc/common-event-format-v25/common-event-format-v25.pdf

```
CEF:Version|Device Vendor|Device Product|Device Version|Device Event Class ID|Name|Severity|[Extension]
```

Here's a real-world instance.

```
CEF:0|Cynet|Cynet 360|4.5.4.22139|0|Memory Pattern - Cobalt Strike Beacon ReflectiveLoader|8| externalId=6 clientId=2251997 scanGroupId=3 scanGroupName=Manually Installed Agents sev=High duser=tikasrv01\\administrator cat=END-POINT Alert dhost=TikaSrv01 src=172.31.5.93 filePath=c:\\windows\\temp\\javac.exe fname=javac.exe rt=3/30/2022 10:55:34 AM fileHash=2BD1650A7AC9A92FD227B2AB8782696F744DD177D94E8983A19491BF6C1389FD rtUtc=Mar 30 2022 10:55:34.688 dtUtc=Mar 30 2022 10:55:32.458 hostLS=2022-03-30 10:55:34 GMT+00:00 osVer=Windows Server 2016 Datacenter x64 1607 epsVer=4.5.5.6845 confVer=637842168250000000 prUser=tikasrv01\\administrator pParams="C:\\Windows\\Temp\\javac.exe" sign=Not signed pct=2022-03-30 10:55:27.140, 2022-03-30 10:52:40.222, 2022-03-30 10:52:39.609 pFileHash=1F955612E7DB9BB037751A89DAE78DFAF03D7C1BCC62DF2EF019F6CFE6D1BBA7 pprUser=tikasrv01\\administrator ppParams=C:\\Windows\\Explorer.EXE pssdeep=49152:2nxldYuopV6ZhcUYehydN7A0Fnvf2+ecNyO8w0w8A7/eFwIAD8j3:Gxj/7hUgsww8a0OD8j3 pSign=Signed and has certificate info gpFileHash=CFC6A18FC8FE7447ECD491345A32F0F10208F114B70A0E9D1CD72F6070D5B36F gpprUser=tikasrv01\\administrator gpParams=C:\\Windows\\system32\\userinit.exe gpssdeep=384:YtOYTIcNkWE9GHAoGLcVB5QGaRW5SmgydKz3fvnJYunOTBbsMoMH3nxENoWlymW:YLTVNkzGgoG+5BSmUfvJMdsq3xYu gpSign=Signed actRem=Kill, Rename
```

VAST's CEF plugin supports parsing such lines using the `cef` format:

```
vast import cef < cef.log
```

VAST translates the `extension` field to a nested record, where the key-value
pairs of the extensions map to record fields. Here is an example of the above
event:

```json {0} title="vast export json '172.31.5.93' | jq"
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

:::note Syslog Header
Sometimes CEF is prefixed with a syslog header. VAST currently only supports the
"raw" form without the syslog header. We are working on support for composable
*generic* formats, e.g., syslog, where the message can basically be any other
existing format.
:::

## Tidbits

This VAST release contains a fair amount of other changes and interesting
improvements. As always, the [changelog][changelog] contains a complete list of
user-facing changes since the last release.

Here are some entries that we want to highlight:

[changelog]: https://vast.io/changelog#v303

### Removing Empty Fields from JSON Output

The `vast export json` command gained new options in addition to the already
existing `--omit-nulls`: Pass `--omit-empty-records`, `--omit-empty-lists`,
or `--omit-empty-maps` to cause VAST not to display empty records, lists, or
maps respectively.

The flag `--omit-empty` empty combines the three new options and `--omit-nulls`,
essentially causing VAST not to render empty values at all. To set these options
globally, add the following to your vast.yaml configuration file:

```yaml
vast:
  export:
    json:
      # Always omit empty records and lists when using the JSON export format,
      # but keep empty lists and maps.
      omit-nulls: true
      omit-empty-records: true
      omit-empty-maps: false
      omit-empty-lists: false
```

### Faster Shutdown

VAST processes now shut down faster, which especially improves the performance
of the `vast import` and `vast export` commands for small amounts of data
ingested or quickly finishing queries.

To quantify this, we've created a database with nearly 300M Zeek events, and ran
an export of a single event with both VAST v2.4.1 and VAST v3.0 repeatedly.

```text {0} title="❯ vast -qq count --estimate | numfmt --grouping"
299,759,532
```

```text {0} title="VAST v2.4.1 ❯ hyperfine --warmup=5 --min-runs=20 'vast -qq --bare-mode export -n1 null'"
Benchmark 1: vast -qq --bare-mode export -n1 null
  Time (mean ± σ):     975.5 ms ±   4.8 ms    [User: 111.2 ms, System: 51.9 ms]
  Range (min … max):   966.3 ms … 985.3 ms    20 runs
```

```text {0} title="VAST v3.0 ❯ hyperfine --warmup=5 --min-runs=20 'vast -qq export -n1 null'"
Benchmark 1: vast -qq --bare-mode export -n1 null
  Time (mean ± σ):     210.8 ms ±   3.5 ms    [User: 99.8 ms, System: 42.5 ms]
  Range (min … max):   204.1 ms … 217.1 ms    20 runs
```

### Connection Stability

VAST clients may now be started before the VAST server: Client processes now
attempt to connect to server processes repeatedly until the configured
connection timeout expires.

We found this to generally improve reliability of services with multiple VAST
clients, for which we often encountered problems with VAST clients being unable
to connect to a VAST server when started before or immediately after the VAST
server.

Additionally, we've fixed a bug that caused VAST to crash when thousands of
clients attempted to connect at around the same time.

### Slim Docker Image

The new `tenzir/vast-slim` Docker image is an alternative to the existing
`tenzir/vast` Docker image that comes in at just under 40 MB in size—less than a
third than the regular image, making it even quicker to get started with VAST.

### Bundled Python Bindings

VAST installations now include Python bindings to VAST as a site package. The
package is called `vast` and also available [separately on PyPI][vast-pypi].

[vast-pypi]: https://pypi.org/project/pyvast

### Expression Short Forms

Extractors can now be used where predicates are expected to test for the
existance of a field or type. For example, `x` and `:T` expand to `x != null`
and `:T != null`, respectively. This pairs nicely with the already existing
short forms for values, e.g., `"foo"` expands to `:string == "foo`.

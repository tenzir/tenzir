---
draft: true
title: FIXME
authors: [dominiklohmann, mavam]
date: 2023-01-20
tags: [release, cef]
---

[VAST v2.5][github-vast-release] is out.

[github-vast-release]: https://github.com/tenzir/vast/releases/tag/v2.5.0

<!--truncate-->

## Common Event Format (CEF) Parser

This release include a new reader plugin for the [Common Event Format
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

```bash
vast import cef < cef.log
```

VAST translates the `extension` field to a nested record, where the key-value
pairs of the extensions map to record fields. Here is an example of the above
event:

```bash
vast export json 172.31.5.93 | jq
```

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

:::note Syslog Header
Sometimes CEF is prefixed with a syslog header. VAST currently only supports the
"raw" form without the syslog header. We are working on support for composable
*generic* formats, e.g., syslog, where the message can basically be any other
existing format.
:::

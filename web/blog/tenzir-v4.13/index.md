---
title: Tenzir v4.13
authors: [dominiklohmann]
date: 2024-05-10
tags: [release, maintenance, performance, leef, syslog]
comments: true
---

We've just released [Tenzir
v4.13](https://github.com/tenzir/tenzir/releases/tag/v4.13.0), a release
focusing on stability and incremental improvements over the [feature-packed past
releases](/blog/tags/release).

![Tenzir v4.13](tenzir-v4.13.excalidraw.svg)

<!-- truncate -->

## Acquire LEEF Over Syslog

Tenzir now speaks LEEF out of the box. The [Log Event Extended Format
(LEEF)][leef] is an event representation popularized by IBM QRadar. Many tools
send LEEF over [Syslog](/formats/syslog).

[leef]: https://www.ibm.com/docs/en/dsm?topic=overview-leef-event-components

LEEF is a line-based format and every line begins with a *header* that is
followed by *attributes* in the form of key-value pairs.

LEEF v1.0 defines 5 header fields and LEEF v2.0 has an additional field to
customize the key-value pair separator, which can be a single character or the
hex value prefixed by `0x` or `x`:

```
LEEF:1.0|Vendor|Product|Version|EventID|
LEEF:2.0|Vendor|Product|Version|EventID|DelimiterCharacter|
```

For LEEF v1.0, the tab (`\t`) character is hard-coded as attribute separator.

Here are some real-world LEEF events:

```
LEEF:1.0|Microsoft|MSExchange|2016|15345|src=10.50.1.1	dst=2.10.20.20	spt=1200
LEEF:2.0|Lancope|StealthWatch|1.0|41|^|src=10.0.1.8^dst=10.0.0.5^sev=5^srcPort=81^dstPort=21
```

Tenzir translates the event attributes into a nested record, where the key-value
pairs map to record fields. Here is an example of the parsed events from above:

```json
{
  "leef_version": "1.0",
  "vendor": "Microsoft",
  "product_name": "MSExchange",
  "product_version": "2016",
  "attributes": {
    "src": "10.50.1.1",
    "dst": "2.10.20.20",
    "spt": 1200,
  }
}
{
  "leef_version": "2.0",
  "vendor": "Lancope",
  "product_name": "StealthWatch",
  "product_version": "1.0",
  "attributes": {
    "src": "10.0.1.8",
    "dst": "10.0.0.5",
    "sev": 5,
    "srcPort": 81,
    "dstPort": 21
  }
}
```

LEEF events typically transmit through Syslog as demonstrated here:

```syslog
<12>Nov 21 13:44:35 LAPTOP-45Q5L6E5 Microsoft-Windows-Security-Mitigations[4340]: LEEF:2.0|Microsoft|Microsoft-Windows-Security-Mitigations|4.6.4640-trial|10|0x09|devTime=2019-11-21 … (truncated for brevity)
```

With Tenzir's [`parse`](/operators/parse) operator, parsing nested data
structures like LEEF in Syslog becomes straightforward. For instance, the
following pipeline reads Syslog containing LEEF over UDP:

```
from udp://127.0.0.1:514 -n read syslog
| parse content leef
```

```json
{
  "facility": 1,
  "severity": 4,
  "timestamp": "Nov 21 13:44:35",
  "hostname": "LAPTOP-45Q5L6E5",
  "app_name": "Microsoft-Windows-Security-Mitigations",
  "process_id": "4340",
  "content": {
    "leef_version": "2.0",
    "vendor": "Microsoft",
    "product_name": "Microsoft-Windows-Security-Mitigations",
    "product_version": "4.6.4640-trial",
    "attributes": {
      "devTime": "2019-11-21T13:44:35.000000",
      // … (truncated for brevity)
    }
  }
}
```

## Cron Scheduling

Expanding on our scheduling capabilities, Tenzir now includes a
[`cron`](https://docs.tenzir.com/next/language/operator-modifiers#cron) operator
modifier enabling precise scheduling using [Crontab
syntax](https://crontab.guru) instead of fixed intervals.

For example, the following pipeline does an API request every Sunday at 04:05 in
the morning:

```
cron "0 5 4 * * SUN" from https://example.com/api
```

## Performance Improvements for Imports

The [`import`](/operators/import) operator introduces a slight delay (up to one
second) in event handling to batch events by schema, substantially enhancing
performance.

![Import Reordering](import-reordering.excalidraw.svg)

## Other Changes

For a full list of enhancements, adjustments, and bug fixes in this release,
please check our [changelog](/changelog#v4130).

Explore the latest features at [app.tenzir.com](https://app.tenzir.com) and
join the conversation on [our Discord server](/discord).

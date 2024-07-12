---
sidebar_custom_props:
  format:
    parser: true
---

# leef

Parses events in the Log Event Extended Format (LEEF).

## Synopsis

```
leef
```

## Description

The [Log Event Extended Format (LEEF)][leef] is an event representation
popularized by IBM QRadar. Many tools send LEEF over [Syslog](syslog.md).

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

## Examples

Read LEEF over a Syslog via UDP:

```
from udp://0.0.0.0:514 read syslog
| parse content leef
| import
```

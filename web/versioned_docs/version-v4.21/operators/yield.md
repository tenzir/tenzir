---
sidebar_custom_props:
  operator:
    transformation: true
---

# yield

Extracts nested records with the ability to unfold lists.

## Synopsis

```
yield <extractor>
```

## Description

The `yield` operator can be used to "zoom into" the extracted part of the
incoming events. It can also return a new event for each element of a list.

### `<extractor>`

The extractor must start with a field name. This can be followed by `.` and
another field name, or by `[]` to extract all elements from the given list.

## Examples

The schema `suricata.dns` provides a list of answers for DNS queries. Assume we
want to extract all answers for `CNAME` records.

```
from eve.json
| where #schema == "suricata.dns"
| yield dns.answers[]
| where rrtype == "CNAME"
```

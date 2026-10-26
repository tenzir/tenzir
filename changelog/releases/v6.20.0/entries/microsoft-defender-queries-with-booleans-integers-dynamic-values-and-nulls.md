---
title: Microsoft Defender queries with booleans, integers, dynamic values, and nulls
type: bugfix
authors:
  - mavam
created: 2026-10-16T15:30:00.000000Z
---

The `from_microsoft_defender` operator failed with an "invalid Microsoft Defender response" error for queries whose results contained booleans, 64-bit integers, dynamic values, or nulls in columns other than strings, which includes most advanced hunting tables. The operator now decodes the format that Microsoft Graph actually returns.

Booleans arrive as `bool`, 64-bit integers as `int64`, `decimal` values as strings that keep their exact digits, `timespan` values as durations, and `dynamic` values as records, lists, or scalars. A `dynamic` string stays a string, even if it looks like a number or holds JSON.

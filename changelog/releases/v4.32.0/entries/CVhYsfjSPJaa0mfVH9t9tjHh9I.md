---
title: "Implement `.?` and `get` for field access without warnings"
type: feature
author: dominiklohmann
created: 2025-04-04T10:36:01Z
pr: 5099
---

The `.?` operator is a new alternative to the `.` operator that allows field
access without warnings when the field does not exist or the parent record is
`null`. For example, both `foo.bar` and `foo.?bar` return `null` if `foo` is
`null`, or if `bar` does not exist, but the latter does not warn about this.
Functionally, `foo.?bar` is equivalent to `foo.bar if foo.has("bar")`.

The `get` method on records or lists is an alternative to index expressions that
allows for specifying a default value when the list index is out of bounds or
the record field is missing. For example, `foo[bar]` is equivalent to
`foo.get(bar)`, and `foo[bar] if foo.has(bar) else fallback` is equivalent to
`foo.get(bar, fallback)`. This works for both records and lists.

Indexing expressions on records now support numeric indices to access record
fields. For example, `this[0]` returns the first field of the top-level record.

The `has` method on records no longer requires the field name to be a constant.

The `config` function replaces the previous `config` operator as a more flexible
mechanism to access variables from the configuration file. If you rely on the
previous behavior, use `from config()` as a replacement.

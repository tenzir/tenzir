---
title: "Improve `json` parser, add `null` type, and various fixes"
type: feature
authors: jachris
pr: 3503
---

The performance of the `json`, `suricata` and `zeek-json` parsers was improved.

The `json` parser has a new `--raw` flag, which uses the raw type of JSON values
instead of trying to infer one. For example, strings with ip addresses are given
the type `string` instead of `ip`.

A dedicated `null` type was added.

Empty records are now allowed. Operators that previously discarded empty records
(for example, `drop`) now preserve them.

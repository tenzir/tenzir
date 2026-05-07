This release makes int64/uint64 column merging lossless during parsing, so fields like `flow_id` that mix signed and unsigned values no longer cause unnecessary table-slice splits. It also extends ocsf::derive to handle list-valued enum fields for full bidirectional OCSF enum normalization.

## 🚀 Features

### OCSF enum list derivation

`ocsf::derive` now derives OCSF enum sibling fields for lists, not just scalar enum fields. For example, DNS answers with `flag_ids: [1, 3, 4]` now also get `flags: ["Authoritative Answer", "Recursion Desired", "Recursion Available"]`, and the reverse direction works for `flags` to `flag_ids` as well.

*By @jachris, @mavam, and @codex in #5354.*

## 🔧 Changes

### Lossless int64/uint64 merging during parsing

Parsing data that mixes `int64` and `uint64` values in the same field no longer produces unnecessary table-slice splits, improving batching performance. Fields like `flow_id` that are always non-negative but occasionally exceed the signed integer limit of `2^63 − 1` are now merged into a single `uint64` column where possible, instead of being emitted as separate slices.

*By @IyeOnline and @claude.*

## 🐞 Bug fixes

### Empty if branches in the new executor

Empty `if` branches no longer crash when running pipelines with the new executor. For example, `if false {}` now behaves like an empty pass-through branch instead of triggering an internal assertion failure.

*By @mavam and @codex in #6128.*

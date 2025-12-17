---
title: "Deduce types for heterogenous JSONL import"
type: feature
author: dominiklohmann
created: 2020-05-26T15:15:36Z
pr: 875
---

When importing events of a new or updated type, VAST now only requires the type
to be specified once (e.g., in a schema file). For consecutive imports, the
event type does not need to be specified again. A list of registered types can
now be viewed using `vast status` under the key `node.type-registry.types`.

When importing JSON data without knowing the type of the imported events a
priori, VAST now supports automatic event type deduction based on the JSON
object keys in the data. VAST selects a type _iff_ the set of fields match a
known type. The `--type` / `-t` option to the `import` command restricts the
matching to the set of types that share the provided prefix. Omitting `-t`
attempts to match JSON against all known types. If only a single variant of a
type is matched, the import falls back to the old behavior and fills in `nil`
for mismatched keys.

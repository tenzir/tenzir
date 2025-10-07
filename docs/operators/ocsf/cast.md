---
title: ocsf::cast
category: OCSF
example: 'ocsf::cast'
---

Casts incoming events to their OCSF type.

```tql
ocsf::cast [encode_variants=bool, null_fill=bool, timestamp_to_ms=bool]
```

## Description

The `ocsf::cast` operator casts incoming events to their corresponding OCSF
event class type. Four fields determine the resulting type:
`metadata.version`, `metadata.profiles`, `metadata.extensions`, and `class_uid`.
The operator casts events sharing the same values for these fields to the same
type. Tenzir supports all OCSF versions (including `-dev` versions), all
profiles, and all event classes. Currently, we limit extensions to those
versioned with OCSF, including the `win` and `linux` extensions.

The operator performs these steps:
- Adds optional fields that don't exist in the original event with a `null`
  value
- Emits warnings for extra fields that shouldn't exist and drops them
- Encodes free-form objects (such as `unmapped`) using their JSON
  representation
- Assigns `@name` based on the class name, for example: `ocsf.dns_activity`

OCSF defines logical types like `timestamp_t` and `subnet_t` that map to
physical base types—here `Long` and `String`. Encodings without native support
for a logical type should use the base type for maximum compatibility. Tenzir
has native types for many OCSF logical types (for example, `time` for
`timestamp_t`), but you may still need the physical base type when working with
downstream tools. The operator provides control knobs like `timestamp_to_ms` to
convert values to their base type representation. This option converts timestamp
values to milliseconds (a number type) even though Tenzir natively supports
timestamps.

Fields that are part of recursive object relationships exhibit one level of
recursion. For example, you can work with `process.parent_process`, but the
nested object `process.parent_process.parent_process` does not exist.

### `encode_variants = bool`

Setting this option to `true` emits free-form objects such as `unmapped` as JSON
strings instead of records. Use it when downstream systems expect a JSON
representation rather than a nested structure.

When `false`, the operator keeps these fields as records, which may lead to
schema drift between events of the same class if the free-form objects differ.
Prefer using `unmapped.parse_json()` to project specific fields when possible.

Defaults to `false`.

### `null_fill = bool`

When `true`, the operator back-fills optional fields that do not exist in the
input with `null` values. This yields schema-complete events for sinks that
expect a full OCSF record. Leave it at the default `false` to avoid materializing
unused fields.

### `timestamp_to_ms = bool`

Converts `time` fields representing OCSF `timestamp` fields into integer values,
counting milliseconds since UNIX epoch.

Defaults to `false`.

## Examples

### Cast a single pre-defined event

```tql
from {
  class_uid: 4001,
  class_name: "Network Activity",
  metadata: {
    version: "1.5.0",
  },
  unmapped: {
    foo: 1,
    bar: 2,
  },
  // … some more fields
}
ocsf::cast null_fill=true encode_variants=true
```
```tql
{
  class_uid: 4001,
  class_name: "Network Activity",
  metadata: {
    version: "1.5.0",
    // … all other metadata fields set to `null`
  },
  unmapped: "{\"foo\": 1, \"bar\": 2}",
  // … other fields (with `null` if they didn't exist before)
}
```

### Homogenize incoming events and send to Clickhouse

```tql
subscribe "ocsf"
where class_name == "Network Activity" and metadata.version == "1.5.0"
ocsf::cast null_fill=true
to_clickhouse table="network_activity"
```

## See Also

[`ocsf::derive`](/reference/operators/ocsf/derive),
[`ocsf::trim`](/reference/operators/ocsf/trim)

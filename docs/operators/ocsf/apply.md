---
title: ocsf::apply
category: OCSF
example: 'ocsf::apply'
---

Casts incoming events to their OCSF type.

```tql
ocsf::apply [preserve_variants=bool]
```

## Description

The `ocsf::apply` operator casts incoming events to their corresponding OCSF
event class type. The resulting type is determined by four fields:
`metadata.version`, `metadata.profiles`, `metadata.extensions` and `class_uid`.
Events sharing the same values for these fields are cast to the same type.
Tenzir supports all OCSF versions (including `-dev` versions), all profiles, and
all event classes. Extensions are currently limited to those versioned with
OCSF, including the `win` and `linux` extensions.

To this end, the operator performs the following steps:
- Add optional fields that are not present in the original event with a `null`
  value
- Emit a warning for extra fields that should not be there and drop them
- Encode free-form objects (such as `unmapped`) using their JSON representation
- Assign `@name` depending on the class name, for example: `ocsf.dns_activity`

The types used for OCSF events are slightly adjusted. For example, timestamps
use the native `time` type instead of an integer representing the number of
milliseconds since the Unix epoch. Fields that are part of recursive object
relationships exhibit one level of recursion. For example, you can work with
`process.parent_process` but the nested object
`process.parent_process.parent_process` does not exist.

### `preserve_variants = bool`

Setting this option to `true` preserves free-form objects such as `unmapped`
as-is, instead of being JSON-encoded. Note that this means the resulting event
schema is no longer consistent across events of the same class, as changes to
these free-form objects lead to different schemas. For schema-consistency and
performance reasons, we recommend keeping this option `false` and instead using
`unmapped.parse_json()` to extract fields on-demand.

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
ocsf::apply
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

### Preserve `unmapped` as a record

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
}
ocsf::apply preserve_variants=true
select unmapped
```
```tql
{
  unmapped: {
    foo: 1,
    bar: 2,
  },
}
```

### Filter, transform and send events to ClickHouse

```tql
subscribe "ocsf"
where class_name == "Network Activity" and metadata.version == "1.5.0"
ocsf::apply
to_clickhouse table="network_activity"
```

## See Also

[`ocsf::derive`](/reference/operators/ocsf/derive),
[`ocsf::trim`](/reference/operators/ocsf/trim)

---
title: ocsf::apply
category: OCSF
example: 'ocsf::apply'
---

Casts incoming events to their OCSF type.

```tql
ocsf::apply
```

## Description

The `ocsf::apply` operator casts incoming events to the type associated with
their OCSF event class. The resulting type is determined based on four fields:
`metadata.version`, `metadata.profiles`, `metadata.extensions` and `class_uid`.
Events that share the same values for those fields will be cast to the same
type. Tenzir supports all OCSF versions (including `-dev` versions), all
profiles and all event classes. Extensions are at the moment limited to those
that are versioned together with OCSF, which includes the `win` and `linux`
extensions.

To this end, the operator performs the following steps:
- Add optional fields that are not present in the original event with a `null` value
- Emit a warning for extra fields that should not be there and drop them
- Encode free-form objects (such as `unmapped`) using their JSON representation
- Assign `@name` depending on the class name, for example: `ocsf.dns_activity`

The types used for OCSF events are slightly adjusted. For example, timestamps
use the native `time` type instead of an integer representing the number of
milliseconds since the Unix epoch. Furthermore, some fields that would lead to
infinite recursion are currently left out. We plan to support recursion up to a
certain depth in the future. Furthermore, this operator will likely be extended
with additional features, such as the ability to drop all optional fields, or to
automatically assign OCSF enumerations based on their sibling ID.

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

### Filter, transform and send events to ClickHouse

```tql
subscribe "ocsf"
where class_name == "Network Activity" and metadata.version == "1.5.0"
ocsf::apply
to_clickhouse table="network_activity"
```

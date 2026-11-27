---
title: Syslog keeps repeated structured-data parameters
type: bugfix
authors:
  - zedoraps
created: 2026-11-23T00:54:41Z
---

The `read_syslog` operator and the `parse_syslog` function again collect the
values of a structured-data parameter that repeats within an element into a
list, as RFC 5424 allows. Since the move to the new event builder, only the
last value survived. Check Point repeats its match-table fields, such as
`layer_name` and `rule_action`, once per matching policy layer:

```tql
from {
  line: r#"<134>1 2026-11-22T10:00:00Z gw CheckPoint 1 - [layer_name:"Network"; layer_name:"Application"]"#,
}
select sd = line.parse_syslog().structured_data
```

```tql
{
  sd: {
    checkpoint_2620: {
      layer_name: [
        "Network",
        "Application",
      ],
    },
  },
}
```

A parameter that occurs once stays a single value, so the same field can be a
single value in one event and a list in another. Parameters under a repeated
SD-ID also collect into a list instead of keeping the last value. A schema
that declares such a field as a list receives all values. A schema that
declares it as a single value receives `null` and a warning.

The Check Point dialect also accepts records that it previously rejected:

- Keys of 32 or more characters, such as `script_value_for_one_time_scripts`.
- A final `;` or spaces before the closing `]`, as in Harmony Endpoint
  exports.
- A tab between a structured-data element and the message.

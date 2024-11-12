---
sidebar_custom_props:
  operator:
    transformation: true
---

# chart

Add metadata to a schema, necessary for rendering as a chart.

## Synopsis

```
chart line [-x|--x-axis <fields>] [-y|--y-axis <field>]
           [--x-axis-type <x-axis-type>]  [--y-axis-type <y-axis-type>]
           [--limit <uint>]
chart area [-x|--x-axis <fields>] [-y|--y-axis <field>]
           [--x-axis-type <x-axis-type>]  [--y-axis-type <y-axis-type>]
           [--limit <uint>]
chart bar  [-x|--x-axis <fields>] [-y|--y-axis <field>]
           [--x-axis-type <x-axis-type>]  [--y-axis-type <y-axis-type>]
           [--limit <uint>]
chart pie  [--name <field>] [--value <fields>]
           [--limit <uint>]
```

## Description

The `chart` operator adds attributes to the schema of the input events,
that are used to guide rendering of the data as a chart.
The operator does no rendering itself.

The `fields` option value is either the name of a single field, or a
comma-separated list of multiple field names, e.g., `foo,bar,baz`.

### `-x|--x-axis <fields>` (`line`, `area`, and `bar` charts only)

Sets the field used for the X-axis.

Values in this field must be strictly increasing (sorted in ascending order,
without duplicates) when creating a `line` or `area` chart, or unique when
creating a `bar` chart.

Defaults to the first field in the schema.

### `-y|--y-axis <fields>` (`line`, `area`, and `bar` charts only)

Sets the fields used for the Y-axis.

Defaults to every field but the first one.

### `--position <position>` (`line`, `area`, and `bar` charts only)

Controls how the values are grouped when rendered as a chart.
Possible values are `grouped` and `stacked`.

Defaults to `grouped`.

### `--x-axis-type <x-axis-type>` (`line`, `area`, and `bar` charts only)

Sets the x-axis scale type.
Possible values are `linear` and `log`.

Defaults to `linear`.

### `--y-axis-type <y-axis-type>` (`line`, `area`, and `bar` charts only)

Sets the y-axis scale type.
Possible values are `linear` and `log`.

Defaults to `linear`.

### `--name <field>` (`pie` chart only)

Sets the field used for the names of the segments.

Values in this field must be unique.

Defaults to the first field in the schema.

### `--value <fields>` (`pie` chart only)

Sets the fields used for the value of a segment.

Defaults to every field but the first one.

### `--limit <uint>`

Limit the chart to `<uint>` data points. This will discard any further events
and raise a warning.

## Examples

Render most common `src_ip` values in `suricata.flow` events as a bar chart:

```
export
| where #schema == "suricata.flow"
| top src_ip
/* -x and -y default to `src_ip` and `count` */
| chart bar
```

Render historical import throughput statistics as a line chart:

```
metrics
| where #schema == "tenzir.metrics.operator"
| where source == true
| summarize bytes=sum(output.approx_bytes) by timestamp resolution 1s
| sort timestamp
| chart line -x timestamp -y bytes --y-axis-type "log"
```

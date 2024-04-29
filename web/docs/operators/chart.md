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
chart area [-x|--x-axis <fields>] [-y|--y-axis <field>]
chart bar  [-x|--x-axis <fields>] [-y|--y-axis <field>]
chart pie  [--name <field>] [--value <fields>]
```

## Description

The `chart` operator adds attributes to the schema of the input events,
that are used to guide rendering of the data as a chart.
The operator does no rendering itself.

### `-x|--x-axis <fields>` (`line`, `area`, and `bar` charts only)

Set the field used for the X-axis.

Values in this field must be strictly increasing
(sorted in ascending order, without duplicates)
when creating a `line` or `area` chart,
or unique when creating a `bar` chart.

 Defaults to the first field in the schema.

### `-y|--y-axis <fields>` (`line`, `area`, and `bar` charts only)

Set the fields used for the Y-axis.
Can either be a single field, or a list of fields spelled with
a list syntax (`[field1, field2]`).

Defaults to every field but the first one.

### `position=<position>` (`line`, `area`, and `bar` charts only)

Control how the values are grouped when rendered as a chart.
Possible values are `grouped` and `stacked`.

Defaults to `grouped`.

### `--name <field>` (`pie` chart only)

Set the field used for the names of the segments.

Values in this field must be unique.

Defaults to the first field in the schema.

### `--value <fields>` (`pie` chart only)

Set the fields used for the value of a segment.
Can either be a single field, or multiple fields delimited with commas
(`field1,field2`).

Defaults to every field but the first one.

## Examples

Render most common `src_ip` values in `suricata.flow` events as a bar chart:

```
export
| where #schema == "suricata.flow"
| top src_ip
/* -x and -y are defaulted to `src_ip` and `count` */
| chart bar
```

Render historical import throughput statistics as a line chart:

```
metrics
| where #schema == "tenzir.metrics.operator"
| where source == true
| summarize bytes=sum(output.approx_bytes) by timestamp resolution 1s
| sort timestamp desc
| chart line -x timestamp -y bytes
```

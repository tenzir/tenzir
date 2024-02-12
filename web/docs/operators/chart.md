---
sidebar_custom_props:
  operator:
    transformation: true
---

# chart

Add metadata to a schema, necessary for rendering as a chart.

## Synopsis

```
chart line   [--title <title>] [-x|--x-axis <field>] [-y|--y-axis <field>]
chart area   [--title <title>] [-x|--x-axis <field>] [-y|--y-axis <field>]
chart bar    [--title <title>] [-x|--x-axis <field>] [-y|--y-axis <field>]
chart pie    [--title <title>] [--name <field>] [--value <field>]
```

## Description

The `chart` operator adds attributes to the schema of the input events,
that are used to guide rendering of the data as a chart.
The operator does no rendering itself.

### `--title <title>`

Set the chart title. Defaults to the schema name.

### `-x|--x-axis <field>` (`line`, `area`, and `bar` charts only)

Set the field used for the X-axis. Defaults to the first field in the schema.

### `-y|--y-axis <field>` (`line`, `area`, and `bar` charts only)

Set the field used for the Y-axis. Defaults to the second field in the schema.

### `--name <field>` (`pie` chart only)

Set the field used for the names of the segments.
Defaults to the first field in the schema.

### `--value <field>` (`pie` chart only)

Set the field used for the value of a segment.
Defaults to the second field in the schema.

## Examples

Render most common `src_ip` values in `suricata.flow` events as a bar chart:

```
export
| where #schema == "suricata.flow"
| top src_ip
/* -x and -y are defaulted to `src_ip` and `count` */
| chart bar --title "Most common src_ip values"
```

Render historical import throughput statistics as a line chart:

```
metrics
| where #schema == "tenzir.metrics.operator"
| where source == true
| summarize bytes=sum(output.approx_bytes) by timestamp resolution 1s
| sort timestamp desc
| chart line -x timestamp -y bytes --title "Import volume over time"
```

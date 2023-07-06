# write

The `write` operator is a short form of the [`to`](to.md) operator that allows
for omitting the connector.

## Synopsis

```
write <format> [to <connector>]
```

## Description

Please refer to the documentation of [`to`](to.md).

## Examples

Write JSON to stdout:

```
write json
```

Write JSON to the Kafka topic `tenzir`:

```
write json to kafka
```

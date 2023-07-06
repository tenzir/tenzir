# read

The `read` operator is a short form of the [`from`](from.md) operator that
allows for omitting the connector.

## Synopsis

```
read <format> [from <connector>]
```

## Description

Please refer to the documentation of [`from`](from.md).

## Examples

Read JSON from stdin:

```
read json
```

Read JSON from the Kafka topic `tenzir`:

```
read json from kafka
```

# Lookup Table

An in-memory hash table with a single key column for enriching with arbitrary
data.

## Synopsis

```
context create  <name> lookup-table
context update  <name> [--key <field>] [--erase]
                       [--create-timeout <duration>]
                       [--update-timeout <duration>]
context delete  <name>
context reset   <name>
context save    <name>
context load    <name>
context inspect <name>
enrich <name>
lookup <name>
```

## Description

A `lookup-table` context behaves like an ordinary table with a unique key column
to be used during enrichment.

:::tip IP and Subnet Optimization
Lookup tables have one special optimization: if you use `subnet` values as key
type, you can probe them with values of type `ip` to perform a longest-prefix
match on all subnets. That is, the lookup table will yield the closest/narrowest
match.

For example, consider a lookup table with keys `10.0.0.0/8` and `10.0.1.0/24`.
If you perform a lookup with IP address `10.0.0.1`, the first subnet will match,
but if you query with `10.0.1.1`, the second subnet will match because it is
more specific.
:::

The following options are currently supported for the `lookup-table` context:

### `--key <field>`

The field in the input that holds the unique key for the lookup table.

Defaults to the first field of the input.

### ``--create-timeout <duration>`

The time after which lookup table entries expire.

Defaults to no timeout.

### `--update-timeout <duration>`

The time after which lookup table entries expire if they are not accessed.

Defaults to no timeout.

### `--erase`

When updating a lookup table, erase fields with matching keys instead of adding
new entries.

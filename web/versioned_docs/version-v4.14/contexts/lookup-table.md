# Lookup Table

An in-memory hash table with a single key column for enriching with arbitrary
data.

## Synopsis

```
context create  <name> lookup-table
context update  <name> --key <field>
context delete  <name>
context reset   <name>
context save    <name>
context load    <name>
context inspect <name>
enrich <name>
lookup <name>
```

## Description

The following options are currently supported for the `lookup-table` context:

### `--key <field>`

The field in the input that holds the unique key for the lookup table.

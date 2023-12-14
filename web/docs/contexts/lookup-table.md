# Lookup Table

An in-memory hash table with a single key column for enriching with arbitrary
data.

## Synopsis

```
context create <name> lookup-table
context update <name> --key <field> [--clear]
context delete <name>
enrich <name> --field <field>
lookup <name> --field <field>
```

## Description

The following options are currently supported for the `lookup-table` context:

### `--key <field>`

The field in the input that holds the unique key for the lookup table.

### `--clear`

Erases all entries in the lookup table before updating.

### `--field <field>`

The name of the field to use as lookup table key.

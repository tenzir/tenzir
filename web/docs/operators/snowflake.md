---
sidebar_custom_props:
  operator:
    source: false
    sink: true
---

# snowflake

Sends events to a [snowflake](https://www.snowflake.com/) database via bulk ingestion.

## Synopsis

```
snowflake <account-identifier> <user-name> <password>
          <snowflake-database> <snowflake schema> <table name>
          [--ingest-mode <create_append | create | append>]
```

## Description

The `snowflake` operator makes it possible to upload events to a snowflake database.

The events are uploaded via bulk-ingestion under the hood and then copied into the target table.

Nested types are supported as
[snowflake semi-structured types](https://docs.snowflake.com/en/sql-reference/data-types-semistructured).
Alternatively, you can use Tenzir's [`flatten`](flatten.md) transformation
operator before the snowflake sink.

Table columns that are not in the event will be null, while event fields
that are not in the table will be dropped.

If the sink creates the table (via `--ingest_mode=create` or `create_append`),
the tables columns are determined by the first event written.

### `<account-identifier>` 

The [snowflake account identifier](https://docs.snowflake.com/en/user-guide/admin-account-identifier) to use.

### `<user-name>` 

The snowflake user name. The user must have the
[`CREATE STAGE`](https://docs.snowflake.com/en/sql-reference/sql/create-stage#access-control-requirements)
privilege on the given schema.

### `<password>`

The password for the user.

### `<snowflake-database>`

The [snowflake database](https://docs.snowflake.com/en/sql-reference/ddl-database) to write to. The user must be allowed to access it.

### `<snowflake schema>`

The [snowflake schema](https://docs.snowflake.com/en/sql-reference/ddl-database) to use. The user be allowed to access it.

### `<table name>`

The name of the table that should be used/created. The user must have the required permissions to create/write to it.

### `--ingest-mode <mode>`

The ingest mode:

* `create_append`: (default) Creates the table if it does not exist, otherwise appends to it.
* `create`: creates the table, causing an error if it already exists.
* `append`: appends to the table, causing an error if it does not exist.

## Examples

Upload `suricata.alert` events to a table `SURICATA_ALERTS` in `MY_DB@TENZIR`:

```
export
| where #schema == "suricata.alert"
| snowflake asldyuf-xgb47555 tenzir_user
  password1234 MY_DB TENZIR SURICATA_ALERTS
```

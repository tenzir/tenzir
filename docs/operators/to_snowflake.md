---
title: to_snowflake
category: Outputs/Events
example: 'to_snowflake account_identifier="…'
---
Sends events to a Snowflake database.

```tql
to_snowflake account_identifier=string, user_name=string, password=string,
             snowflake_database=string snowflake_schema=string table=string,
             [ingest_mode=string]
```

:::note
This operator is currently only available in the amd64 Docker images.
:::

## Description

The `to_snowflake` operator makes it possible to send events to a
[Snowflake](https://www.snowflake.com/) database. It uploads the events via
bulk-ingestion under the hood and then copies them into the target table.

The operator supports nested types as [Snowflake semi-structured
types](https://docs.snowflake.com/en/sql-reference/data-types-semistructured).
Alternatively, you can use the [`flatten`](/reference/functions/flatten) function
operator beforehand.

### `account_identifier = string`

The [Snowflake account
identifier](https://docs.snowflake.com/en/user-guide/admin-account-identifier)
to use.

### `user_name = string`

The Snowflake user name. The user must have the [`CREATE
STAGE`](https://docs.snowflake.com/en/sql-reference/sql/create-stage#access-control-requirements)
privilege on the given schema.

### `password = string`

The password for the user.

### `database = string`

The [Snowflake database](https://docs.snowflake.com/en/sql-reference/ddl-database)
to write to. The user must be allowed to access it.

### `schema = string`

The [Snowflake schema](https://docs.snowflake.com/en/sql-reference/ddl-database)
to use. The user be allowed to access it.

### `table = string`

The name of the table that should be used/created. The user must have the required
permissions to create/write to it.

Table columns that are not in the event will be null, while event fields that
are not in the table will be dropped. Type mismatches between the table and
events are a hard error.

### `ingest_mode = string (optional)`

You can set the ingest mode to one of three options:

- `"create_append"`: Creates the table if it does not exist, otherwise
  appends to it.
- `"create"`: creates the table, causing an error if it already exists.
- `"append"`: appends to the table, causing an error if it does not exist.

In case the operator creates the table it will use the the first event to infer
the columns.

Default to `"create_append"`.

## Examples

### Send an event to a Snowflake table

Upload `suricata.alert` events to a table `TENZIR` in `MY_DB@SURICATA_ALERT`:

```tql
export
where @name == "suricata.alert"
to_snowflake \
  account_identifier="asldyuf-xgb47555",
  user_name="tenzir_user",
  password="password1234",
  database="MY_DB",
  schema="SURICATA_ALERT",
  table="TENZIR"
```

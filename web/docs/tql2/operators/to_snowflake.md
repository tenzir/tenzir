# to_snowflake

Sends events to a [snowflake](https://www.snowflake.com/) database via bulk ingestion.

```tql
to_snowflake account_identifier=string, user_name=string, password=string,
             snowflake_database=string snowflake_schema=string table=string,
             [ingest_mode=string]
```

## Description

The `snowflake` operator makes it possible to upload events to a snowflake database.

The events are uploaded via bulk-ingestion under the hood and then copied into the target table.

Nested types are supported as
[snowflake semi-structured types](https://docs.snowflake.com/en/sql-reference/data-types-semistructured).
Alternatively, you can use Tenzir's [`flatten` function](../functions/flatten.md)
operator before the snowflake sink.

Table columns that are not in the event will be null, while event fields
that are not in the table will be dropped. Type mismatches between the table and
events are a hard error.

### `account_identifier = string`

The [snowflake account identifier](https://docs.snowflake.com/en/user-guide/admin-account-identifier) to use.

### `user_name = string`

The snowflake user name. The user must have the
[`CREATE STAGE`](https://docs.snowflake.com/en/sql-reference/sql/create-stage#access-control-requirements)
privilege on the given schema.

### `password = string`

The password for the user.

### `database = string`

The [snowflake database](https://docs.snowflake.com/en/sql-reference/ddl-database)
to write to. The user must be allowed to access it.

### `schema = string`

The [snowflake schema](https://docs.snowflake.com/en/sql-reference/ddl-database)
to use. The user be allowed to access it.

### `table = string`

The name of the table that should be used/created. The user must have the required
permissions to create/write to it.

Table columns that are not in the event will be null, while event fields
that are not in the table will be dropped. Type mismatches between the table and
events are a hard error.

### `ingest_mode = string (optional)`

The ingest mode must be one of three options:

* `"create_append"`: (default) Creates the table if it does not exist, otherwise appends to it.
* `"create"`: creates the table, causing an error if it already exists.
* `"append"`: appends to the table, causing an error if it does not exist.

If the table is created by the operator, its columns will be inferred from the
first event.

## Examples

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

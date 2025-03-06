# to_clickhouse

Sends events to a ClickHouse table.

```tql
to_clickhouse table=string, [url=string, user=string, password=string,
                             mode=string, primary=field,
                             tls=bool, cacert=string, certfile=string, keyfile=string]
```

## Description

### `table = string`

The name of the table you want to write to. When giving a plain table name, it
will use the `default` database, otherwise `database.table` can be specified.

### `url = string (optional)`

The location of the ClickHouse Server.

Defaults to `"localhost:9000"`.

### `user = string (optional)`

The user to use for authentication.

Defaults to `"default"`.

### `password = string (optional)`

The password for the given user.

Defaults to `""`.

### `mode = string (optional)`

* `"create"` if you want to create a table and fail if it already exists
* `"append"` to append to an existing table
* `"create_append"` to create a table if it does not exist and append to it
  otherwise.

Defaults to `"create_append"`.

### `primary = field (optional)`

The primary key to use when creating a table. Required for `mode = "create"` as
well as for `mode = "create_append"` if the table does not yet exist.

### `tls = bool (optional)`

Enables TLS.

Defaults to `false`.

### `skip_peer_verification = bool (optional)`

Toggles TLS certificate verification.

Defaults to `false`.

### `cacert = string (optional)`

Path to the CA certificate used to verify the server's certificate.

### `certfile = string (optional)`

Path to the client certificate.

### `keyfile = string (optional)`

Path to the key for the client certificate.

## Types

Tenzir uses ClickHouse's [clickhouse-cpp](https://github.com/ClickHouse/clickhouse-cpp)
client library to communicate with ClickHouse. The below table explains the
translation from Tenzir's types to ClickHouse:

| Tenzir | ClickHouse | Comment |
|:--- | :--- | :--- |
| `int64` | `Int64` | |
| `uint64` | `Unt64` | |
| `double` | `Float64` | |
| `ip` | `IPv6` | |
| `subnet` | `Tuple(ip IPv6, length UInt8)` | |
| `time` | `DateTime64(9)` | |
| `duration` | `Int64` | Converted as `nanoseconds(duration)` |
| `record` | `Tuple(...)` | Fields in the tuple will be named with the field name |
| `list<T>` | `Array(T)` | |

Tenzir also supports `Nullable` versions of the above types (or their nested types).

### Table Creation

When a ClickHouse table is created from Tenzir, all columns except the `primary`
will be created as `Nullable`.

The table will be created from the first event the operator receives.

## Examples

### Send CSV file to a local ClickHouse instance

```tql
from "my_file.csv"
to_clickhouse table="my_table"
```

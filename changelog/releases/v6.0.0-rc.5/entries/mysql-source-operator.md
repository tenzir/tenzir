---
title: MySQL source operator
type: feature
authors:
  - mavam
  - claude
pr: [5721, 5738]
created: 2026-02-06T08:53:45.097588Z
---

The `from_mysql` operator lets you read data directly from MySQL databases.

Read a table:

```tql
from_mysql table="users", host="localhost", port=3306, user="admin", password="secret", database="mydb"
```

List tables:

```tql
from_mysql show="tables", host="localhost", port=3306, user="admin", password="secret", database="mydb"
```

Show columns:

```tql
from_mysql table="users", show="columns", host="localhost", port=3306, user="admin", password="secret", database="mydb"
```

And ultimately execute a custom SQL query:

```tql
from_mysql sql="SELECT id, name FROM users WHERE active = 1",
           host="localhost",
           port=3306,
           user="admin",
           password="secret",
           database="mydb"
```

The operator supports TLS/SSL connections for secure communication with MySQL
servers. Use `tls=true` for default TLS settings, or pass a record for
fine-grained control:

```tql
from_mysql table="users", host="db.example.com", database="prod", tls={
  cacert: "/path/to/ca.pem",
  certfile: "/path/to/client-cert.pem",
  keyfile: "/path/to/client-key.pem",
}
```

The operator supports MySQL's `caching_sha2_password` authentication method and automatically maps MySQL data types to Tenzir types.

Use `live=true` to continuously stream new rows from a table. The operator
tracks progress using a watermark on an integer column, polling for rows above
the last-seen value:

```tql
from_mysql table="events", live=true, host="localhost", database="mydb"
```

By default, the tracking column is auto-detected from the table's
auto-increment primary key. To specify one explicitly:

```tql
from_mysql table="events", live=true, tracking_column="event_id",
           host="localhost", database="mydb"
```

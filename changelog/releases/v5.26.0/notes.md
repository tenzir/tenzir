This release introduces the from_mysql operator for reading data directly from MySQL databases, with support for live streaming, custom SQL queries, and TLS connections. It also adds link-based HTTP pagination and optional field parameters for user-defined operators.

## 🚀 Features

### Link header pagination for HTTP operators

The `paginate` parameter for the `from_http` and `http` operators now supports link-based pagination via the `Link` HTTP header.

Previously, pagination was only available through a lambda function that extracted the next URL from response data. Now you can use `paginate="link"` to automatically follow pagination links specified in the response's `Link` header, following RFC 8288. This is useful for APIs that use HTTP header-based pagination instead of embedding next URLs in the response body.

The operator parses the `Link` header and follows the `rel=next` relation to automatically fetch the next page of results.

Example:

```
from_http "https://api.example.com/data", paginate="link"
```

If an invalid pagination mode is provided (neither a lambda nor `"link"`), the operator now reports a clear error message.

*By @mavam and @claude.*

### MySQL source operator

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

The operator supports TLS/SSL connections for secure communication with MySQL servers. Use `tls=true` for default TLS settings, or pass a record for fine-grained control:

```tql
from_mysql table="users", host="db.example.com", database="prod", tls={
  cacert: "/path/to/ca.pem",
  certfile: "/path/to/client-cert.pem",
  keyfile: "/path/to/client-key.pem",
}
```

The operator supports MySQL's `caching_sha2_password` authentication method and automatically maps MySQL data types to Tenzir types.

Use `live=true` to continuously stream new rows from a table. The operator tracks progress using a watermark on an integer column, polling for rows above the last-seen value:

```tql
from_mysql table="events", live=true, host="localhost", database="mydb"
```

By default, the tracking column is auto-detected from the table's auto-increment primary key. To specify one explicitly:

```tql
from_mysql table="events", live=true, tracking_column="event_id",
           host="localhost", database="mydb"
```

*By @mavam and @claude in #5721 and #5738.*

### Optional field parameters for user-defined operators

User-defined operators in packages can now declare optional field-type parameters with `null` as the default value. This allows operators to accept field selectors that are not required to be provided.

When a field parameter is declared with `type: field` and `default: null`, you can omit the argument when calling the operator, and the parameter will receive a `null` value instead. You can then check whether a field was provided by comparing the parameter to `null` within the operator definition.

Example:

In your package's operator definition, declare an optional field parameter:

```yaml
args:
  named:
    - name: selector
      type: field
      default: null
```

In the operator implementation, check if the field was provided:

```tql
set result = if $selector != null then "field provided" else "field omitted"
```

When calling the operator, the field argument becomes optional:

```tql
my_operator                    # field is null
my_operator selector=x.y       # field is x.y
```

Only `null` is allowed as the default value for field parameters. Non-null defaults are rejected with an error during package loading.

*By @mavam and @claude in #5753.*

## 🐞 Bug fixes

### Improve write_lines operator performance

We have significantly improved the performance of the `write_lines` operator.

*By @IyeOnline.*

### merge() function recursive deep merge for nested records

The `merge()` function now performs a recursive deep merge when merging two records. Previously, nested fields were dropped when merging, so `merge({hw: {sn: "XYZ123"}}, {hw: {model: "foobar"}})` would incorrectly produce `{hw: {model: "foobar"}}` instead of recursively merging the nested fields. The function now correctly produces `{hw: {sn: "XYZ123", model: "foobar"}}` by materializing both input records and performing a deep merge on them.

*By @mavam and @claude in #5728.*

### Secret type support for user-defined operator parameters

User-defined operators in packages can now declare parameters with the `secret` type to ensure that secret values are properly handled as secret expressions:

```
args:
  positional:
    - name: api_key
      type: secret
      description: "API key to use for authentication"
```

*By @mavam and @claude in #5752.*

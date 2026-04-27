This release adds OData pagination support to the from_http operator, enabling seamless iteration over Microsoft Graph and other OData v4 collection responses.

## 🚀 Features

### OData pagination for from_http

The `from_http` operator now supports `paginate="odata"` for [OData](https://www.oasis-open.org/standard/odata-v4-01-os/) collection responses such as Microsoft Graph:

```tql
from_http "https://graph.microsoft.com/v1.0/users",
  headers={"ConsistencyLevel": "eventual"},
  paginate="odata" {
  read_json
}
```

This mode emits the objects from the response body's top-level `value` array and follows top-level `@odata.nextLink` URLs until no next link is present. The next link can be absolute or relative to the current response URL.

*By @mavam and @codex.*

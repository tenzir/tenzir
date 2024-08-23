# save_http

Writes a bytes stream to a HTTP URL.

```tql
save_http url:str, [method=str, params=record, headers=record]
```

## Description

### `url: str`

The `url` to write bytes to.

### `method = str (optional)`

The HTTP method to use.

### `params = record (optional)`

The query parameters for the request.

### `headers = record (optional)`

The headers for the request.

## Examples

```tql
subscribe "active-threats"
save_http "url", method="PUT", headers = { API_KEY: "000-000-000-000" }
```


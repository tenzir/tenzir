# save_http

Sends a byte stream via HTTP.

```tql
save_http url:string, [method=string, params=record, headers=record]
```

## Description

The `save_http` operator performs a HTTP request with the request body being the
bytes provided by the previous operator.

### `url: string`

The URL to write to. The `http://` scheme can be omitted.

### `method = string (optional)`

The HTTP method, such as `POST` or `GET`.

The default is `"POST"`.

### `params = record (optional)`

The query parameters for the request.

### `headers = record (optional)`

The headers for the request.

## Examples

### Call a webhook with pipeline data

```tql
save_http "example.org/api", headers={"X-API-Token": "0000-0000-0000"}
```

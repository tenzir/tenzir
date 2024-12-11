# load_http

Loads a byte stream via HTTP.

```tql
load_http url:string, [method=string, params=record, headers=record]
```

## Description

The `save_http` operator performs a HTTP request and returns the response.

### `url: string`

The URL to request from. The `http://` scheme can be omitted.

### `method = string (optional)`

The HTTP method, such as `POST` or `GET`.

The default is `"GET"`.

### `params = record (optional)`

The query parameters for the request.

### `headers = record (optional)`

The headers for the request.

## Examples

### Perform an API call and get the response

```tql
load_http "example.org/api", headers={"X-API-Token": "0000-0000-0000"}
```

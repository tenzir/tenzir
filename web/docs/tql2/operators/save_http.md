# save_http

Sends a byte stream via HTTP.

```tql
save_http url:string, [params=record, headers=record, method=string,
          skip_peer_verification=bool, skip_hostname_verification=bool,
          verbose=bool]
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

### `skip_peer_verification = bool (optional)`

Whether to skip TLS peer verification.

Defaults to `false`.

### `skip_hostname_verification = bool (optional)`

Whether to skip TLS peer verification.

Defaults to `false`.

<!-- ### `verbose = bool (optional)` -->
<!---->
<!-- Whether to emit verbose output. -->
<!---->
<!-- Defaults to `false`. -->

## Examples

### Call a webhook with pipeline data

```tql
save_http "example.org/api", headers={"X-API-Token": "0000-0000-0000"}
```

## See Also

[`load_http`](load_http.md)

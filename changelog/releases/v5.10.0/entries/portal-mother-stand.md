---
title: "HTTP request body encoding"
type: feature
author: raxyte
created: 2025-07-07T12:45:45Z
pr: 5305
---

The `from_http` and `http` operators now support using `record` values for the
request `body` parameter. By default, the record is serialized as JSON. You can
also specify `encode="form"` to send the body as URL-encoded form data. When
using `form` encoding, nested fields are flattened using dot notation (e.g.,
`foo: {bar: "baz"}` => `foo.bar=baz`). This supersedes the `payload` parameter,
which therefore is now deprecated.

###### Examples

By default, setting `body` to a record will JSON-encode it:

```tql
http "https://api.example.com/data", body={foo: "bar", count: 42}
```
```http
POST /data HTTP/1.1
Host: api.example.com
Content-Type: application/json
Content-Length: 33

{
  "foo": "bar",
  "count": 42
}
```

To change the encoding, you can use the `encode` option:

```tql
http "https://api.example.com/data",
  body={foo: {bar: "baz"}, count: 42},
  encode="form"
```
```http
POST /data HTTP/1.1
Host: api.example.com
Content-Type: application/x-www-form-urlencoded
Content-Length: 20

foo.bar=baz&count=42
```

Arbitrary body contents can be sent by using a string or blob:

```tql
http "https://api.example.com/data", body="hello world!"
```
```http
POST /data HTTP/1.1
Host: api.example.com
Content-Length: 12

hello world!
```

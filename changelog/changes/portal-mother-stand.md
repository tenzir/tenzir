---
title: "HTTP request body encoding"
type: feature
authors: raxyte
pr: 5305
---

The `from_http` and `http` operators now support using `record` values for the
request `body` parameter. By default, the record is serialized as JSON. You can
also specify `encode="form"` to send the body as URL-encoded form data. When
using `form` encoding, nested fields are flattened using dot notation (e.g.,
`foo: {bar: "baz"}` => `foo.bar=baz`)

**Examples**

- **Send a JSON body (default):**  
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

- **Send as URL-encoded form data:**  
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

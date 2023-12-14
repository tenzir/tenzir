---
sidebar_custom_props:
  connector:
    loader: true
---

# http

Loads bytes via HTTP.

## Synopsis

```
http [<method>] <url> [<item>..]
```

## Description

The `http` loader performs a HTTP request and returns the bytes of the HTTP
response body.

We modeled the `http` loader after [HTTPie](https://httpie.io/), which comes
with an expressive and intuitive command-line syntax. We recommend to study the
[HTTPie documentation](https://httpie.io/docs/cli/examples) to understand the
full extent of the command-line interface. In many cases, you can perform an
*exact* copy of the HTTPie command line and use it drop-in with the HTTP loader,
e.g.,  the invocation

```bash
http PUT pie.dev/put X-API-Token:123 foo=bar
```

becomes

```
from http PUT pie.dev/put X-API-Token:123 foo=bar
```

More generally, if your HTTPie command line is `http X` then you can write `from
http X` to obtain an event stream or `load http X` for a byte stream.

### `<method>`

The HTTP method, such as `POST` or `GET`.

The argument is optional and its default value depends on the command line:

- `GET` for requests without body
- `POST` for requests with body

For example, the following operator configurations are identical:

```
from http GET pie.dev/get
from http pie.dev/get
```

Similarly, when we provide data for the request body, the following two
invocations are identical:

```
from http POST pie.dev/post foo=bar
from http pie.dev/post foo=bar
```

### `<url>`

The HTTP request URL.

The scheme is `http://` and can be omitted from the argument. For example, the
following two invocations are identical:

```
from http pie.dev/get
from http http://pie.dev/get
```

### `<item>`

A HTTP request item in the form of a key-value pair.

The character separating the key-value pair determines the semantics of the
item:

- `key:value` a HTTP header with name `key` and value `value`
- `key=value` a data field in the HTTP request body with `key` as string key and
  `value` as string value
- `key:=value` a data field in the HTTP request body with `key` as string key and
  `value` as JSON value

Use `\` to escape characters that shouldn't be treated as separators.

## Examples

Download and process a [CSV](../formats/csv.md) file:

```
from http example.org/file.csv read csv
```

Process a Zstd-compressed [Zeek TSV](../formats/zeek-tsv.md) file:

```
load http example.org/gigantic.log.zst
| decompress zstd
| read zeek-tsv
```

Send a HTTP PUT request with a `X-API-Token` header and body of `{"foo": 42}`:

```
from http PUT pie.dev/put X-API-Token:123 foo=42
```

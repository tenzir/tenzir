---
title: uuid
category: Utility
example: 'uuid()'
---

Generates a Universally Unique Identifier (UUID) string.

```tql
uuid([version=string]) -> string
```

## Description

The `uuid` function generates a [Universally Unique Identifier
(UUID)](https://en.wikipedia.org/wiki/Universally_unique_identifier) string.
UUIDs, 128-bit numbers, uniquely identify information in computer systems.

This function generates several UUID versions based on relevant standards like
[RFC 4122](https://www.rfc-editor.org/rfc/rfc4122.html) and the newer [RFC
9562](https://www.rfc-editor.org/rfc/rfc9562.html) which defines versions 6 and
7.

### `version = string (optional)`

Specifies the version of the UUID to generate. If you omit this argument, the
function uses `"v4"` by default. It supports the following values:

- `"v1"`: Generates a time-based UUID using a timestamp and node ID.
- `"v4"`: Generates a randomly generated UUID using a cryptographically strong
  random number generator (see RFC 4122). **This is the default.**
- `"v6"`: Generates a time-based UUID, similar to v1 but reordered for better
  database index locality and lexical sorting (see RFC 9562).
- `"v7"`: Generates a time-based UUID using a Unix timestamp and random bits,
  designed to be monotonically increasing (suitable for primary keys, see RFC
  9562).
- `"nil"`: Generates the special "nil" UUID, which consists entirely of zeros:
  `00000000-0000-0000-0000-000000000000`.

Defaults to `"v4"`.

## Examples

### Generate a random default (v4) UUID

```tql
from {guid: uuid()}
```

```tql
{guid: "f47ac10b-58cc-4372-a567-0e02b2c3d479"}
```

### Generate a random version 7 UUID

```tql
from {guid: uuid(version="v7")}
```

```tql
{guid: "018ecb4f-abc1-7123-8def-0123456789ab"}
```

### Generate the nil UUID

```tql
from {guid: uuid(version="nil")}
```

```tql
{guid: "00000000-0000-0000-0000-000000000000"}
```

## See Also

[`random`](/reference/functions/random)

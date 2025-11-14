---
title: encode_base58
category: Encoding
example: 'encode_base58("Tenzir")'
---

Encodes bytes as Base58.

```tql
encode_base58(bytes: blob|string) -> string
```

## Description

Encodes bytes as Base58.

### `bytes: blob|string`

The value to encode as Base58.

## Examples

### Encode a string as Base58

```tql
from { input: "Hello World" }
encoded = input.encode_base58()
```

```tql
{
  input: "Hello World",
  encoded: "JxF12TrwUP45BMd",
}
```

## See Also

[`decode_base58`](/reference/functions/decode_base58)

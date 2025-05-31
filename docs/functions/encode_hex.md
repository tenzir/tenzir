---
title: encode_hex
category: Encoding
example: 'encode_hex("Tenzir")'
---
Encodes bytes into their hexadecimal representation.

```tql
encode_hex(bytes: blob|string) -> string
```

## Description

Encodes bytes into their hexadecimal representation.

### `bytes: blob|string`

The value to encode.

## Examples

### Encode a string to hex

```tql
from {bytes: "Tenzir"}
encoded = bytes.encode_hex()
```

```tql
{bytes: "Tenzir", encoded: "54656E7A6972"}
```

## See Also

[`decode_hex`](/reference/functions/decode_hex)

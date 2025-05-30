---
title: encode_base64
---

Encodes bytes as Base64.

```tql
encode_base64(bytes: blob|string) -> string
```

## Description

Encodes bytes as Base64.

### `bytes: blob|string`

The value to encode as Base64.

## Examples

### Encode a string as Base64

```tql
from {bytes: "Tenzir"}
encoded = bytes.encode_base64()
```

```tql
{bytes: "Tenzir", encoded: "VGVuemly"}
```

## See Also

[`decode_base64`](/reference/functions/decode_base64)

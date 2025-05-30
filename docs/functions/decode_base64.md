---
title: decode_base64
---

Decodes bytes as Base64.

```tql
decode_base64(bytes: blob|string) -> blob
```

## Description

Decodes bytes as Base64.

### `bytes: blob|string`

The value to decode as Base64.

## Examples

### Decode a Base64 encoded string

```tql
from {bytes: "VGVuemly"}
decoded = bytes.decode_base64()
```

```tql
{bytes: "VGVuemly", decoded: "Tenzir"}
```

## See Also

[`encode_base64`](/reference/functions/encode_base64)

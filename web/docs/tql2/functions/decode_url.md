# decode_url

Decodes URL encoded strings.

```tql
decode_url(string: blob|string) -> blob
```

## Description

Decodes URL encoded strings or blobs, converting percent-encoded sequences back
to their original characters.

### `string: blob|string`

The URL encoded string to decode.

## Examples

### Decode a URL encoded string

```tql
from {input: "Hello%20World%20%26%20Special%2FChars%3F"}
decoded = input.decode_url()
```

```tql
{input: "Hello%20World%20%26%20Special%2FChars%3F", decoded: "Hello World & Special/Chars?"}
```

## See Also

[`encode_url`](encode_url.md)

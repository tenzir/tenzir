# pseudonymize

Pseudonymizes IP addresses according to the [Crypto-PAn](https://en.wikipedia.org/wiki/Crypto-PAn) algorithm.

## Parameters

- `seed: [string]`: The 64-byte seed that describes a hexadecimal value. When the seed is shorter than 64 bytes, the operator will append zeros to match the size; when it is longer, it will truncate the seed.
- `fields: <list>`: The list of fields to apply the pseudonymization to. During pseudonymization, the operator will ignore specified fields that do not contain IP addresses.

## Example

```yaml
pseudonymize:
  seed: abcdef1234567890987654321fedcbaabcdef1234567890987654321fedcbaab
  fields:
    - ip_origin
    - ip_dest
```

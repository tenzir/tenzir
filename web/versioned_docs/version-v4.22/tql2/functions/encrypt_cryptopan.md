# encrypt_cryptopan

Pseudonymizes fields according to a given method.

## Synopsis

```tql
encrypt_cryptopan(address:ip, [seed=str])
```

## Description

Anonymizes IP Addresses using the
[Crypto-PAn](https://en.wikipedia.org/wiki/Crypto-PAn) algorithm.

### `address: ip`

The IP Address to anonymize.

### `seed = str (optional)`

A 64-byte seed that describes a hexadecimal value. When the seed is shorter than
64 bytes, the operator will append zeros to match the size; when it is longer,
it will truncate the seed.

## Example

Anonymize all values of the fields `src_ip` and `dst_ip` using the
`crypto-pan` algorithm and the seed `"deadbeef"`:

```tql
from { src_ip: 114.13.11.35, dst_ip: 114.56.11.200 }
src_ip = encrypt_cryptopan(src_ip, seed="deadbeef")
dst_ip = encrypt_cryptopan(dst_ip, seed="deadbeef")
write_json
```

```json title="Anonymized"
{
  "src_ip": "117.179.11.60",
  "dst_ip": "117.135.244.180"
}
```

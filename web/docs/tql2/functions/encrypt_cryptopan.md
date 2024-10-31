# encrypt_cryptopan

Encrypts an IP address via Crypto-PAn.

## Synopsis

```tql
encrypt_cryptopan(address:ip, [seed=str])
```

## Description

The `encrypt_cryptopan` function encrypts the IP `address` using the
[Crypto-PAn](https://en.wikipedia.org/wiki/Crypto-PAn) algorithm.

### `address: ip`

The IP address to encrypt.

### `seed = str (optional)`

A 64-byte seed that describes a hexadecimal value. When the seed is shorter than
64 bytes, the function appends zeros to match the size; when it is longer, it
truncates the seed.

## Examples

### Encrypt IP address fields

```tql
let $seed = "deadbeef" // use secret() function in practice
from {
  src: encrypt_cryptopan(114.13.11.35, seed=$seed),
  dst: encrypt_cryptopan(114.56.11.200, seed=$seed),
}
```

```tql
{
  src: 117.179.11.60,
  dst: 117.135.244.180,
}
```

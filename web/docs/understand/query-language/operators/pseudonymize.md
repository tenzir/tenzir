# pseudonymize

Pseudonymizes IP addresses according to a certain method. Currently, VAST only
uses the [Crypto-PAn](https://en.wikipedia.org/wiki/Crypto-PAn) algorithm.

## Parameters

- `method: [string]`: The method to pseudonymize the address. As of now, this
value always defaults to `crypto-pan`.
- `seed: [string]`: The 64-byte seed that describes a hexadecimal value. When
the seed is shorter than 64 bytes, the operator will append zeros to match the
size; when it is longer, it will truncate the seed.
- `fields: <list>`: The list of fields to apply the pseudonymization to. During
pseudonymization, the operator will ignore specified fields that do not contain
IP addresses.

## Example

```yaml
pseudonymize:
  method: crypto-pan
  seed: abcdef1234567890987654321fedcbaabcdef1234567890987654321fedcbaab
  fields:
    - ip_origin
    - ip_dest
```

## Pipeline Operator String Syntax (Experimental)

```
pseudonymize [-m|--method=STRING] -s|--seed=STRING FIELD[, …]
```
### Example
```
pseudonymize --method="crypto-pan" --seed="deadbeef" ip_origin, ip_dest
```

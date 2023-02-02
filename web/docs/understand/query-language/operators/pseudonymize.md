# pseudonymize

Pseudonymizes IP addresses according to a certain method. Currently, VAST only
uses the [Crypto-PAn](https://en.wikipedia.org/wiki/Crypto-PAn) algorithm.

:::warning Unstable
We plan to change the `pseudonymize` operator into a function usable in an
upcoming `put` operator, removing the need removing the need for `pseudonymize`
as an operator.
:::

## Synopsis

```
pseudonymize (-m|--method=STRING) (-s|--seed=STRING) FIELDS[, …]
```

### Method

The method to pseudonymize the address. As of now, this value always defaults to
`crypto-pan`.

### Seed

The 64-byte seed that describes a hexadecimal value. When the seed is shorter
than 64 bytes, the operator will append zeros to match the size; when it is
longer, it will truncate the seed.

### Fields

The list of fields to apply the pseudonymization to. During pseudonymization,
the operator will ignore specified fields that do not contain IP addresses.

## Example

Pseudonymize all values of the fields `ip_origin` and `ip_dest` using the
`crypto-pan` algorithm and `deadbeef` seed:

```
pseudonymize --method="crypto-pan" --seed="deadbeef" ip_origin, ip_dest
```

## YAML Syntax Example

:::info Deprecated
The YAML syntax is deprecated since VAST v3.0, and will be removed in a future
release. Please use the pipeline syntax instead.
:::

```yaml
pseudonymize:
  method: crypto-pan
  seed: abcdef1234567890987654321fedcbaabcdef1234567890987654321fedcbaab
  fields:
    - ip_origin
    - ip_dest
```

# hash

Computes a SHA256 hash digest of a given field.

:::warning Unstable
We plan to change the `hash` operator into a function usable in an upcoming
`put` operator, removing the need for `hash` as an operator.
:::

## Synopsis

```
hash (-s|--salt=STRING) FIELD
```

### Salt

A salt value for the hash.

### Field

The field name over which the hash is computed.

## Example

Hash all values of the field `username` using the salt value `"xxx"` and store
the digest in a new field `username_hashed`:

```
hash --salt="B3IwnumKPEJDAA4u" username
```

## YAML Syntax Example

:::info Deprecated
The YAML syntax is deprecated since VAST v3.0, and will be removed in a future
release. Please use the pipeline syntax instead.
:::

```yaml
hash:
  field: username
  out: pseudonym
  salt: "B3IwnumKPEJDAA4u"
```

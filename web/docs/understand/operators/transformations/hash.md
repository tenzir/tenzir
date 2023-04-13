# hash

Computes a SHA256 hash digest of a given field.

:::warning Experimental
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

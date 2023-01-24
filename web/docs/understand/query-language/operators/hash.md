# hash

Computes a SHA256 hash digest of a given field.

## Parameters

- `field: string`: the field name over which the hash is computed.
- `out: string`: the field name in which to store the digest.
- `salt: string`: a salt value for the hash. *(optional)*

## Example

```yaml
hash:
  field: username
  out: pseudonym
  salt: "B3IwnumKPEJDAA4u"
```

## Pipeline Operator String Syntax (Experimental)

```
hash (-s|--salt=STRING) FIELD
```

### Example

Hash all values of the field `username` using the salt value `"xxx"` and store
the digest in a new field `username_hashed`:

```
hash --salt="B3IwnumKPEJDAA4u" username
```

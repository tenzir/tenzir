---
sidebar_custom_props:
  operator:
    transformation: true
---

# hash

Computes a SHA256 hash digest of a given field.

:::warning Deprecated
This operator will soon be removed in favor of first-class support for functions
that can be used in a variety of different operators and contexts.
:::

## Synopsis

```
hash [-s|--salt=<string>] <field>
```

## Description

The `hash` operator calculates a hash digest of a given field.

### `<-s|--salt>=<string>`

A salt value for the hash.

### `<field>`

The field over which the hash is computed.

## Examples

Hash all values of the field `username` using the salt value `"xxx"` and store
the digest in a new field `username_hashed`:

```
hash --salt="B3IwnumKPEJDAA4u" username
```

---
sidebar_custom_props:
  operator:
    transformation: true
---

# pseudonymize

Pseudonymizes fields according to a given method.

:::warning Deprecated
This operator will soon be removed in favor of first-class support for functions
that can be used in a variety of different operators and contexts.
:::

## Synopsis

```
pseudonymize -m|--method=<string> -s|--seed=<seed> <extractor>...
```

## Description

The `pseudonimize` operator replaces IP address using the
[Crypto-PAn](https://en.wikipedia.org/wiki/Crypto-PAn) algorithm.

Currently, `pseudonimize` exclusively works for fields of type `ip`.

### `-m|--method=<string>`

The algorithm for pseudonimization

### `-s|--seed=<seed>`

A 64-byte seed that describes a hexadecimal value. When the seed is shorter than
64 bytes, the operator will append zeros to match the size; when it is longer,
it will truncate the seed.

### `<extractor>...`

The list of extractors describing fields to pseudonomize. If an extractor
matches types other than IP addresses, the operator will ignore them.

## Example

Pseudonymize all values of the fields `src_ip` and `dest_ip` using the
`crypto-pan` algorithm and `deadbeef` seed:

```
pseudonymize --method="crypto-pan" --seed="deadbeef" src_ip, dest_ip
```

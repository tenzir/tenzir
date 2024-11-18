# Bloom Filter

A space-efficient data structure to represent large sets.

## Synopsis

```
context create  <name> bloom-filter --capacity <capacity> --fp-probability <probability>
context update  <name> [--key <field>]
context delete  <name>
context reset   <name>
context save    <name>
context load    <name>
context inspect <name>
enrich <name>
lookup <name>
```

## Description

The `bloom-filter` context is a [Bloom
filter](https://en.wikipedia.org/wiki/Bloom_filter) that stores large sets data
in a compact way, at the cost of false positives during lookup.

The Bloom filter has two tuning knobs:

1. **Capacity**: the maximum number of items in the filter.
2. **False-positive probability**: the chance of reporting an indicator not in
   the filter.

These two parameters dictate the space usage of the Bloom filter. Consult Thomas
Hurst's [Bloom Filter Calculator](https://hur.st/bloomfilter/) for finding the
optimal configuration for your use case.

Bloom filter terminology commonly uses the following parameter abbreviations:

| Parameter | Name | Description
|:---------:|------|--------------
| `n` | Capacity | The maximum number of unique elements that guarantee the configured false-positive probability
| `m` | Size | The number of bits that the Bloom filter occupies
| `p` | False positive probability | The probability of erroneously reporting an element to be in the set

The Bloom filter implementation is a C++ rebuild of DCSO's
[bloom](https://github.com/DCSO/bloom) library. It is binary-compatible and uses
the exact same method for FNV1 hashing and parameter calculation, making it a
drop-in replacement for `bloom` users.

### `--capacity <capacity>`

The maximum number of unique items the Bloom filter can hold while guaranteeing
the configured false-positive probability.

### `--fp-probability <probability>`

The probability of a false positive when looking up an item in the Bloom filter.

Must be within `0.0` and `1.0`.

### `--key <field>`

The field in the input to be inserted into the Bloom filter.

If an element exists already in the Bloom filter, the update operation is a
no-op.

Defaults to the first field of the input.

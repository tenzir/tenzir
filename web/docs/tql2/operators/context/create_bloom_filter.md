# create_bloom_filter

Creates a Bloom filter context.

```tql
context::create_bloom_filter name:string capacity:uint fp_probability:float
```

## Description

The `context::create_bloom_filter` operator constructs a new context of type
[Bloom filter](../../../enrichment/README.md#bloom-filter).

To find suitable values for the capacity and false-positive probability, Consult
Thomas Hurst's [Bloom Filter Calculator](https://hur.st/bloomfilter/). The
parameter `p` corresponds to `capacity` and `p` to `fp_probability`.

You can also create a Bloom filter context as code by adding it to
`tenzir.contexts` in your `tenzir.yaml`:

```yaml {0} title="<prefix>/etc/tenzir/tenzir.yaml"
tenzir:
  contexts:
    my-iocs:
      type: bloom-filter
      arguments:
        capacity: 1B
        fp-probability: 0.001
```

Making changes to `arguments` of an already created context has no effect.

### `name : string`

The name of the new Bloom filter.

### `capacity : uint`

The maximum number of items in the filter that maintain the false positive
probility. Adding more elements does not yield an error, but lookups will
more likely return false positives.

### `fp_probability : float`

The false-positive probability of the Bloom filter.

## Examples

### Create a new Bloom filter context

```tql
context::create_bloom_filter "ctx"
```

## See Also

[`context::create_lookup_table`](create_lookup_table.md),
[`context::create_geoip`](create_geoip.md),
[`context::enrich`](enrich.md),
[`context::inspect`](inspect.md),
[`context::load`](load.md),
[`context::remove`](remove.md),
[`context::reset`](update.md),
[`context::save`](save.md),
[`context::update`](update.md),

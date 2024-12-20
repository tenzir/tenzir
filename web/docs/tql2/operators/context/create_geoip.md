# create_geoip

Creates a GeoIP context.

```tql
context::create_geoip name:string, [db_path=string]
```

## Description

The `context::create_geoip` operator constructs a new context of type
[GeoIP](../../../enrichment/README.md#geoip).

You must either provide a database with the `db_path` argument or use
[`context::load`](load.md) to populate the context after creation.

You can also create a GeoIP context as code by adding it to `tenzir.contexts` in
your `tenzir.yaml`:

```yaml {0} title="<prefix>/etc/tenzir/tenzir.yaml"
tenzir:
  contexts:
    my-geoips:
      type: geoip
      arguments:
        db-path: /usr/local/share/stuff/high-res-geoips.mmdb
```

Making changes to `arguments` of an already created context has no effect.

### `name: string`

The name of the new GeoIP context.

### `db_path = string (optional)`

The path to the [MMDB](https://maxmind.github.io/MaxMind-DB/) database, relative
to the node's working directory.

## Examples

### Create a new GeoIP context

```tql
context::create_geoip "ctx", db_path="GeoLite2-City.mmdb"
```

### Populate a GeoIP context from a remote location

Load [CIRCL's Geo
Open](https://data.public.lu/en/datasets/geo-open-ip-address-geolocation-per-country-in-mmdb-format/)
dataset from November 12, 2024:

```tql
load_http "https://data.public.lu/fr/datasets/r/69064b5d-bf46-4244-b752-2096b16917a4"
context::load "ctx"
```

## See Also

[`context::create_lookup_table`](create_lookup_table.md),
[`context::create_bloom_filter`](create_bloom_filter.md),
[`context::enrich`](enrich.md),
[`context::erase`](erase.md),
[`context::inspect`](inspect.md),
[`context::list`](list.md),
[`context::load`](load.md),
[`context::remove`](remove.md),
[`context::reset`](update.md),
[`context::save`](save.md),
[`context::update`](update.md),

---
title: create_geoip
category: Contexts
example: 'context::create_geoip "ctx", db_path="GeoLite2-City.mmdb"'
---
Creates a GeoIP context.

```tql
context::create_geoip name:string, [db_path=string]
```

## Description

The `context::create_geoip` operator constructs a new context of type
[GeoIP](/explanations/enrichment#geoip-database).

You must either provide a database with the `db_path` argument or use
[`context::load`](/reference/operators/context/load) to populate the context after creation.

You can also create a GeoIP context as code by adding it to `tenzir.contexts` in
your `tenzir.yaml`:

```yaml title="<prefix>/etc/tenzir/tenzir.yaml"
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

[`context::create_bloom_filter`](/reference/operators/context/create_bloom_filter),
[`context::create_lookup_table`](/reference/operators/context/create_lookup_table),
[`context::inspect`](/reference/operators/context/inspect),
[`context::load`](/reference/operators/context/load),
[`context::remove`](/reference/operators/context/remove),
[`context::save`](/reference/operators/context/save),
[`enrich`](/reference/operators/context/enrich),
[`erase`](/reference/operators/context/erase),
[`list`](/reference/operators/context/list),
[`reset`](/reference/operators/context/reset),
[`update`](/reference/operators/context/update)

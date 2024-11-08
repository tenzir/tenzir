# write_zeek_tsv

Transforms event stream into Zeek Tab-Separated Value byte stream.

```tql
write_zeek_tsv [set_separator=str, empty_field=str, unset_field=str, disable_timestamp_tags=bool]
```

## Description

The [Zeek](https://zeek.org) network security monitor comes with its own
tab-separated value (TSV) format for representing logs. This format includes
additional header fields with field names, type annotations, and additional
metadata.

The `write_zeek_tsv` operator (re)generates the TSV metadata based on
Tenzir's internal schema. Tenzir's data model is a superset of
Zeek's, so the conversion into Zeek TSV may be lossy. The Zeek types `count`,
`real`, and `addr` map to the respective Tenzir types `uint64`, `double`, and
`ip`.

### `set_separator = str (optional)`

Specifies the set separator.

Defaults to `\x09`.

### `empty_field = str (optional)`

Specifies the separator for empty fields.

Defaults to `(empty)`.

### `unset_field = str (optional)`

Specifies the separator for unset "null" fields.

Defaults to `-`.

### `disable_timestamp_tags = bool (optional)`

Disables the `#open` and `#close` timestamp tags.

Defaults to `false`.

## Examples

Write filtered Zeek `conn.log` to a topic:

```tql
subscribe "zeek-logs"
where duration > 2s and id.orig_p != 80
publish "threat-alerts"
```

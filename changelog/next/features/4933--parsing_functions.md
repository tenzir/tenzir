It is now possible to define additional patterns in the `parse_grok` function.

The manually defined header for the `read_xsv` family of parsers can now be
specified as a list of strings, instead of a single, correctly delimited string.

The additional `pattern_definitions` for `read_grok` can now be specified as
either a `record` or `string`.

The new `parse_csv`, `parse_kv`, `parse_ssv`, `parse_tsv` and `parse_xsv` functions can be
used to parse strings in their respective formats.

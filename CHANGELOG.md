# Changelog

This changelog documents all notable user-facing changes of VAST.

Every entry has a category for which we use the following visual abbreviations:

- 🎁 = feature
- 🔄 = change
- 🐞 = bugfix

## [Unreleased]

- 🔄 The default directory name for persistent state changed from `vast` to
  `vast.db`. This makes it possible to run `./vast` in the current directory
  without having to specify a different state directory on the command line.

- 🔄 Nested types are from now on accessed by the `.`-syntax. This means
  VAST now has a unified syntax to select nested types and fields.
  For example, what used to be `zeek::http` is now just `zeek.http`.

- 🎁 Data extractors in the query language can now contain a type prefix.
  This enables an easier way to extract data from a specific type. For example,
  a query to look for Zeek conn log entries with responder IP address 1.2.3.4
  had to be written with two terms, `#type == zeek.conn && id.resp_h == 1.2.3.4`,
  because the nested id record can occur in other types as well. Such queries
  can now written more tersely as `zeek.conn.id.resp_h == 1.2.3.4`.

- 🎁 VAST gained support for importing Suricata JSON logs. The import command
  has a new suricata format that can ingest EVE JSON output.

- 🎁 The data parser now supports `count` and `integer` values according to the
  *International System for Units (SI)*. For example, `1k` is equal to `1000`
  and `1Ki` equal to `1024`.

- 🐞 The `map` data parser did not parse negative values correctly. It was not
  possible to parse strings of the form `"{-42 -> T}"` because the parser
  attempted to parse the token for the empty map `"{-}"` instead.

- 🎁 VAST can now ingest JSON data. The `import` command gained the `json`
  format, which allows for parsing line-delimited JSON (LDJSON) according to a
  user-supplied schema with `--schema` or `--schema-file`. The JSON objects in
  the input must match a congruent record type in the schema, that is, the keys
  of the JSON object must be equal to the record field names and the object
  values must be convertible to the record field types.

- 🐞 The CSV printer of the `export` command used to insert 2 superfluous
  fields when formatting an event: The internal event ID and a deprecated
  internal timestamp value. Both fields have been removed from the output,
  bringing it into line with the other output formats.

- 🔄 The (internal) option `--node` for the `import` and `export` commands
  has been renamed from `-n` to `-N`, to allow usage of `-n` for
  `--max-events`.

- 🎁 For symmetry to the `export` command, the `import` command gained the
  `--max-events,n` option to limit the number of events that will be imported.

- 🔄 To make the export option to limit the number of events to be exported
  more idiomatic, it has been renamed from `--events,e` to `--max-events,n`.
  Now `vast export -n 42` generates at most 42 events.

- 🐞 When a node terminates during an import, the client process remained
  unaffected and kept processing input. Now the client terminates when a
  remote node terminates.

- 🎁 The `import` command gained the `--listen,l` option to receive input
  from the network. Currently only UDP is supported. Previously, one had to use
  a clever netcat pipe with enough receive buffer to achieve the same effect,
  e.g., `nc -I 1500 -p 4200 | vast import pcap`. Now this pipe degenerates to
  `vast import pcap -l`.

- 🎁 The new `--disable-accounting` option shuts off periodic gathering of
  system telemetry in the accountant actor. This also disables output in the
  `accounting.log`.

- 🐞 Evaluation of predicates with negations return incorrect results. For
  example, the expression `:addr !in 10.0.0.0/8` created a disjunction of all
  fields to which `:addr` resolved, without properly applying De-Morgan. The
  same bug also existed for key extractors. De-Morgan is now applied properly
  for the operations `!in` and `!~`.


## [0.1] - 2019-02-28

This is the first official release.

[0.1]: https://github.com/vast-io/vast/releases/tag/0.1

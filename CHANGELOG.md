# Changelog

This changelog documents all notable user-facing changes of VAST.

Every entry has a category for which we use the following visual abbreviations:

- 🎁 = feature
- 🔄 = change
- 🐞 = bugfix

## [Unreleased]

- 🎁 The `import` command learned the `json` format. Since json is a
  very generic data format, it can only be used in conjunction with
  a user supplied schema, via either of the `--schema` or `--schema-file`
  options.

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

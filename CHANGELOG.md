# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [Unreleased]

### Added
- The new `--disable-accounting` option can shut off periodic gathering of system telemetry.
- The `import` command gained the `--listen,l` option to receive input
  from the network. Currently only UDP is supported.
- The `import` command gained the `--max-events,n` option to limit the number of events
  that will be imported.

### Changed
- Terminate running import processes if the server goes down.
- Fix evaluation of predicates with negations and type or key extractors.
- The short option for the `--node` option of the import and export commands
  has been renamed from `-n` to `-N`.
- The export option to limit the number of events to be exported has been
  renamed from `--events,e` to `--max-events,n`.


## [0.1] - 2019-02-28

This is the first official release.

[0.1]: https://github.com/vast-io/vast/releases/tag/0.1

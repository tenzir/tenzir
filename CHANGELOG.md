# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [Unreleased]

### Added
- The new `--disable-accounting` option can shut off periodic gathering of system telemetry.
- The `import` command gained the `--listen,l` option to receive input
  from the network. Currently only UDP is supported.

### Changed
- Fix evaluation of predicates with negations and type or key extractors.


## [0.1] - 2019-02-28

This is the first official release.

[0.1]: https://github.com/vast-io/vast/releases/tag/0.1

# Changelog

This changelog documents all notable changes to the summarize plugin for VAST.
## 2.0.0

### Breaking Changes

- The `aggregate` plugin is now called `summarize`.
  [#2228](https://github.com/tenzir/vast/pull/2228)

### Bug Fixes

- The `summarize` no longer fails when its configuration does not match the
  events it's operating on at all, i.e., when all columns are unrelated, and
  instead ignores such events.
  [#2258](https://github.com/tenzir/vast/pull/2258)

## v1.0.0

This is the first official release.

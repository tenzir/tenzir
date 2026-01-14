# Publishing Bugfix

This release fixes a bug where the `publish` operator could drop events.

## 🐞 Bug fixes

### Fixed `publish` operator dropping events

The `publish` operator could drop events during flush operations. Events are now reliably delivered to subscribers.

*By @raxyte in #5618.*

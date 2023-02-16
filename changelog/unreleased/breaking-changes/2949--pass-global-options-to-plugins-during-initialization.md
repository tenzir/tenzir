There is a small addition to the plugin interface - the plugin initialization
in `initialize()` now uses two `record` parameters. The former contains all
plugin-relevant, the latter contains absolutely all configuration options. The
plugins can now use both parameters for read-only access to all options in
VAST.

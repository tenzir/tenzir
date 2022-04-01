VAST has now complete support for passing environment variables as alternate
path to configuration files. Environment variables have *lower* precedence than
CLI arguments and *higher* precedence than config files. Variable names of the
form `VAST_FOO__BAR_BAZ` map to `vast.foo.bar-baz`, i.e., `__` is a record
separator and `_` translates to `-`. This does not apply to the prefix `VAST_`,
which is considered the application identifier. Only variables with non-empty
values are considered.

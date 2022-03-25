VAST has now complete support for passing environment variables as alternate
path to configuration files. Environment variables have lower precedence than
CLI arguments and higher precedence than config files. Non-empty variables
of the form `VAST_FOO_BAR__BAZ` map to `vast.foo.bar_baz`, i.e.,
double-escaping `_` yields a literal `_` in the config key. VAST only considers
variables with the prefix `VAST_` and `CAF_`.

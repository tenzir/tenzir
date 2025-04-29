Packages gained a new `config.disabled` option that causes all pipelines and
contexts in the package to be disabled.

Packages, pipelines and contexts now support a new `disabled-env` option besides
the `disabled` options that take the name of an environment variable, that, when
set to `true` or `false`, overrides the `disabled` option.

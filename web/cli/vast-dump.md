The `dump` command prints configuration and schema-related information. By
default, the output is JSON-formatted. The flag `--yaml` flag switches to YAML
output.

For example, to see all registered concept definitions, use the following
command:

```
vast dump concepts
```

To dump all models in YAML format, use:

```
vast dump --yaml models
```

Specifying `dump` alone without a subcommand shows the concatenated output from
all subcommands.

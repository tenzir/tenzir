# CLI Plugins folder

Any python module (file) added to this folder will be loaded by the main script
and the PyInvoke tasks defined in it will be added to the CLI.

You can add [PyInvoke
configurations](https://docs.pyinvoke.org/en/stable/concepts/configuration.html#default-configuration-values)
to the module by defining them in an `INVOKE_CONFIG` top level variable.

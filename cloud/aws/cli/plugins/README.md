# CLI Plugins folder

Any python module (file) added to this folder will be loaded by the main script
and the PyInvoke tasks defined in it will be added to the CLI.

When creating tasks in a plugin, the decorators and objects defined in the
module `vast_invoke` should be used instead of the plain `invoke` constructs.
This enables proper handling of the PTY by the entrypoint script and Docker.

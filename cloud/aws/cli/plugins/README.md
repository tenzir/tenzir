# CLI Plugins folder

Any python module (file) added to this folder will be loadable by the main
script. Plugins must be activated using the VAST_CLOUD_PLUGINS variable:
```
VAST_CLOUD_PLUGINS = workbucket,tests
```
The PyInvoke tasks defined in the activated plugins will automatically be added
to the CLI. If a terraform module exists with the same name as the plugin, it will also be included by the `deploy` and `destroy` commands.

Each plugin can define a list of [dynaconf](https://www.dynaconf.com/)
validators in a top level `VALIDATORS` variable. These validations will be
loaded for all activated plugins.

When creating tasks in a plugin, the decorators and objects defined in the
module `vast_invoke` should be used instead of the plain `invoke` constructs.
This enables proper handling of the PTY by the entrypoint script and Docker.

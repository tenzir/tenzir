---
sidebar_position: 1
---

# Configure

Threat Bus uses a configuration file that contains both global and
plugin-specific settings. This section discusses the general layout of the file
and options you can configure.

## Config File

Threat Bus' configuration file is formatted in YAML and consists of two sections,
`logging` and `plugins`. The following example explains the general structure.

```yaml
logging:
  console: true
  console_verbosity: DEBUG
  file: false
  file_verbosity: DEBUG
  filename: threatbus.log

plugins:
  backbones:
    inmem:
  apps:
    zeek:
      host: "127.0.0.1"
      port: 47761
      module_namespace: Tenzir
```

For a comprehensive list of available options, see the `config.yaml.example` file
that is shipped together with Threat Bus.

### Logging Configuration

Logging is configured globally. The main application forwards the logging
settings to all installed plugins. Threat Bus supports colored console logs and
file logging. File and console logging are independent.

### Plugin Configuration

The `plugins` section contains all plugin specific configuration settings. The
section differentiates `backbones` and `apps`, depending on the
plugin type.

Plugin configuration is managed via the plugin name. For example, the plugin
`threatbus-zeek` has the name `zeek` and is an `app` plugin. Thus it is
configured in a section called `zeek`, below the `apps` section in the config.

The options that can be configured per plugin are defined by the plugin itself.
Check the [plugin documentation](understand/plugins) for details on the
individual plugins.

#### Disabling of Installed Plugins

Threat Bus automatically becomes aware of all plugins that are installed on the
same host system or virtual environment. However, plugins must have a non-empty
section in the `config.yaml` to get loaded. You can "disable" any installed
plugin simply by not putting it into the config file.

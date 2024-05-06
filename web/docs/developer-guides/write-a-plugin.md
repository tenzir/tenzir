# Write a Plugin

Implementing a new plugin requires the following steps:

1. [Setup the scaffolding](#setup-the-scaffolding)
2. [Choose a plugin type](#choose-a-plugin-type)
3. [Implement the plugin interface](#implement-the-plugin-interface)
4. [Process configuration options](#process-configuration-options)
5. [Compile the source code](#compile-the-source-code)
6. [Add unit and integration tests](#add-unit-and-integration-tests)
7. [Package it](#package-it)

Next, we'll discuss each step in more detail.

## Setup the scaffolding

The scaffolding of a plugin includes the CMake glue that makes it possible to
use as static or dynamic plugin.

Pass `-DTENZIR_ENABLE_STATIC_PLUGINS:BOOL=ON` to `cmake` to build plugins
alongside Tenzir as static plugins. This option is always on for static binary
builds.

Tenzir ships with many plugins that showcase what a typical scaffold looks like.
Have a look at the the
[plugins](https://github.com/tenzir/tenzir/tree/main/plugins) directory, and an
[example `CMakeLists.txt` file from the AMQP
plugin](https://github.com/tenzir/tenzir/blob/main/plugins/amqp/CMakeLists.txt).

We highly urge calling the provided `TenzirRegisterPlugin` CMake in your plugin's
`CMakeLists.txt` file instead of handrolling your CMake build scaffolding
code. This ensures that your plugin always uses the recommended defaults.
Non-static installations of Tenzir contain the `TenzirRegisterPlugin.cmake`
modules.

The typical structure of a plugin directory includes the following
files/directories:

- `README.md`: An overview of the plugin and how to use it.
- `CHANGELOG.md`: A trail of user-facing changes.
- `schema/`: new schemas that ship with this plugin.
- `<plugin>.yaml.example`: the configuration knobs of the plugin. We comment out
  all options by default so that the file serves as reference. Users can
  uncomment specific settings they would like to adapt.

  The CMake build scaffolding installs all of the above files/directories, if
  present.

## Choose a plugin type

Tenzir offers [a variety of customization
points](../architecture/plugins.md#plugin-types), each of which defines its
own API by inheriting from the plugin base class `tenzir::plugin`. When writing
a new plugin, you can choose a subset of available types by inheriting from the
respective plugin classes.

:::caution Dreaded Diamond
To avoid common issues with multiple inheritance, all intermediate plugin
classes that inherit from `tenzir::plugin` use *virtual inheritance* to avoid
issues with the [dreaded
diamond](https://isocpp.org/wiki/faq/multiple-inheritance#mi-diamond).
:::

## Implement the plugin interface

After having the necessary CMake in place, you can now derive from one or more
plugin base classes to define your own plugin. Based on the chosen plugin
types, you must override one or more virtual functions with an implementation
of your own.

The basic anatomy of a plugin class looks as follows:

```cpp
class example_plugin final : public virtual component_plugin,
                             public virtual command_plugin {
public:
  /// Loading logic.
  example_plugin();

  /// Teardown logic.
  ~example_plugin() override;

  /// Initializes a plugin with its respective entries from the YAML config
  /// file, i.e., `plugin.<NAME>`.
  /// @param plugin_config The relevant subsection of the configuration.
  /// @param global_config The entire Tenzir configuration for potential access
  /// to global options.
  caf::error initialize(const record& plugin_config,
                        const record& global_config) override;

  /// Returns the unique name of the plugin.
  std::string name() const override;

  // TODO: override pure virtual functions from the base classes.
  // ...
};
```

The plugin constructor should only perform minimal actions to instantiate a
well-defined plugin instance. In particular, it should not throw or perform any
operations that may potentially fail. For the actual plugin ramp up, please use
the `initialize` function that processes the user configuration. The purpose of
the destructor is to free any used resources owned by the plugin.

Each plugin must have a unique name. This returned string should consicely
identify the plugin internally.

Please consult the documentation specific to each plugin type above to figure
out what virtual function need overriding. In the above example, we have a
`command_plugin` and a `component_plugin`. This requires implementing the
following two interfaces:

```cpp
component_plugin_actor make_component(
  node_actor::stateful_pointer<node_state> node) const override;

std::pair<std::unique_ptr<command>, command::factory>
make_command() const override;
```

After completing the implementation, you must now register the plugin. For
example, to register the `example` plugin, include the following line after the
plugin class definition:

```cpp
// This line must not be in a namespace.
TENZIR_REGISTER_PLUGIN(tenzir::plugins::example_plugin)
```

:::tip Registering Type IDs
The example plugin also shows how to register additional type IDs with the actor
system configuration, which is a requirement for sending custom types from the
plugin between actors. For more information, please refer to the CAF
documentation page [Configuring Actor Applications: Adding Custom Message
Types](https://actor-framework.readthedocs.io/en/stable/ConfiguringActorApplications.html#adding-custom-message-types).
:::

## Process configuration options

To configure a plugin at runtime, Tenzir first looks whether the YAML
configuration contains a key with the plugin name under the top-level key
`plugins`. Consider our example plugin with the name `example`:

```yaml
plugins:
  example:
    option: 42
```

Here, the plugin receives the record `{option: 42}` at load time. A plugin can
process the configuration snippet by overriding the following function of
`tenzir::plugin`:

```
caf::error initialize(const record& plugin_config,
                      const record& global_config) override;
```

Tenzir expects the plugin to be fully operational after calling `initialize`.
Subsequent calls to the implemented customization points must have a
well-defined behavior.

## Compile the source code

### Building alongside Tenzir

When configuring the Tenzir build, you need to tell CMake the path to the plugin
source directory. The CMake variable `TENZIR_PLUGINS` holds a comma-separated
list of paths to plugin directories.

To test that Tenzir loads the plugin properly, you can use `tenzir
--plugins=example version` and look into the `plugins`. A key-value pair with
your plugin name and version should exist in the output.

Refer to the [plugin loading](../configuration.md#load-plugins) section of
the documentation to find out how to explicitly de-/activate plugins.

### Building against an installed Tenzir

It is also possible to build plugins against an installed Tenzir. The
`TenzirRegisterPlugin` CMake function contains the required scaffolding to set
up `test` and `integration` targets that mimic Tenzir's targets. Here's how you
can use it:

```bash
# Configure the build. Requires Tenzir to be installed in the CMake Module Path.
cmake -S path/to/plugin -B build
# Optionally you can manually specify a non-standard Tenzir install root:
#   TENZIR_DIR=/opt/tenzir cmake -S path/to/plugin -B build
cmake --build build
# Run plugin-specific unit tests.
ctest --test-dir build
# Install to where Tenzir is also installed.
cmake --install build
# Optionally you can manually specify a non-standard Tenzir install root:
#   cmake --install build --prefix /opt/tenzir
# Run plugin-specific integration tests against the installed Tenzir.
cmake --build build --target integration
```

## Add unit and integration tests

Tenzir comes with unit and integration tests. So does a robust plugin
implementation. We now look at how you can hook into the testing frameworks.

### Unit tests

Every plugin ideally comes with unit tests. The `TenzirRegisterPlugin` CMake
function takes an optional `TEST_SOURCES` argument that creates a test binary
`<plugin>-test` with `<plugin>` being the plugin name. The test binary links
against the `tenzir::test` target. ou can find the test binary in `bin` within
your build directory.

To execute registered unit tests, you can also simply run the test binary
`<plugin>-test`, where `<plugin>` is the name of your plugin. The build target
`test` sequentially runs tests for all plugins and Tenzir itself.

### Integration tests

Every plugin ideally comes with integration tests as well. Our convention is
that integration tests reside in an `integration` subdirectory. If you add a
file called `integration/*.bats`, Tenzir runs them alongside the regular
integration tests.

Note that plugins may affect the overall behavior of Tenzir. Therefore we
recommend to to run all integrations regularly by running the build target
`integration`.

To execute plugin-specific integration tests only, run the build target
`integration-<plugin>`, where `<plugin>` is the name of your plugin.

## Package it

If you plan to publish your plugin, you may want to create a GitHub repository.
Please let us know if you do so, we can then link to community plugins from the
documentation.

:::tip Contribute Upstream
If you think your plugin provides key functionality beneficial to all Tenzir
users, feel free to [submit a pull
request](https://github.com/tenzir/tenzir/pulls) to the main repository. But
please consider swinging by our [community chat](/discord) or
starting a [GitHub Discussion](https://github.com/tenzir/tenzir/discussions) to
ensure that your contribution becomes a fruitful addition. 🙏
:::

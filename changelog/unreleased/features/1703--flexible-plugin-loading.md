The options `vast.plugins` and `vast.plugin-dirs` may now be specified on the
command line as well as the configuration. Use the options `--plugins` and
`--plugin-dirs` respectively.

The build-time CMake option `VAST_ENABLE_PLUGIN_AUTOLOADING` is superseded by a
runtime option to load all bundled plugins, i.e., static or dynamic plugins
built alongside VAST. Add the special plugin name `bundled` to `vast.plugins` to
enable this feature, or use `--plugins=bundled` on the command line. Adding
`all` causes all bundled and external plugins to be loaded.

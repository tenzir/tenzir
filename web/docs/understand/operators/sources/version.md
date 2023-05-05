# version

Returns a single event displaying version information of VAST.

## Synopsis

```
version [--dev]
```

## Description

The version operator returns detailed information about VAST. The output of the
operator has the following schema:

```
record {
  // The version number of VAST.
  version: string,
  // A list of plugins. Excludes builtins.
  plugins: list<record {
    // The plugin name.
    name: string,
    // The types of the plugins, e.g., printer and parser.
    types: list<string>,
  }>,
}
```

### `--dev`

Add additional, developer-facing information to the output of the operator. With
`--dev` set, the operator's output has the following schema:

```
record {
  // The version number of VAST.
  version: string,
  // Build information for VAST.
  build: record {
    // The configured build type; one of Debug, RelWithDebInfo or Release.
    type: string,
    // A hash that uniquely describes the VAST build tree.
    tree_hash: string,
    // Whether assertions are enabled.
    assertions: bool,
    // Information about enabled sanitizers.
    sanitizers: record {
      // Whether ASan is enabled.
      address: bool,
      // Whether UBSan is enabled.
      undefined_behavior: bool,
    },
  },
  // A list of plugins. Includes builtins.
  plugins: list<record {
    // The plugin name.
    name: string,
    // The types of the plugins, e.g., printer and parser.
    types: list<string>,
    // The kind of the plugin; one of static, dynamic, or builtin.
    kind: string,
    // The version of the plugin.
    version: string,
  }>,

}
```

## Examples

Get the version of your VAST process.

```
version
```

Get extended version information of the remote VAST node.

```
remote version --dev
```

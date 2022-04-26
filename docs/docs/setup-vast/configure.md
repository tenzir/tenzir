---
sidebar_position: 2
---

# Configure
VAST offers several mechanisms to adjust configuration options on startup.

1. Command-line arguments
2. Environment variables
3. Configuration files
4. Compile-time defaults

These mechanisms are sorted by precedence, i.e., command-line arguments override
environment variables, which override configuration file settings.

Compile-time defaults can only be changed by [rebuilding VAST from
source](/docs/setup-vast/build).

## Command Line

VAST has a hierarchical command structure of this form:

```bash
vast [opts] cmd1 [opts1] cmd2 [opts2] ...
```

Both long `--long=X` and short `-s X` exist. Boolean options do not require
explicit specification of a value, and it suffices to write `--long` and `-s`
to set an option to true.

:::info Hierarchical Options
Each command has its own dedicated set of options. Options are not global and
only valid for their respective command. Consider this example:

```bash
vast --option foo # option applies to command 'vast'
vast foo --option # option applies to command 'foo'
```
:::

### Getting help

Internally, VAST implicitly generates three sub-commands for every command:

1. `help` for displaying brief usage
2. `documentation` for more more details and description
3. `manual` for rendering a man page

You get short usage instructions for every command by adding the `help`
sub-command or providing the option `--help` (which has the shorthand `-h`):

```bash
vast help
vast --help
vast -h
```

The same help pattern applies to (sub-)commands:

```bash
vast export help
vast export --help
vast export -h
```

In addition to brief usage instructions, the `documentation` command provides
more comprehensive information. The output is in Markdown
([CommonMark](https://commonmark.org/)) format. We recommend using
[glow](https://github.com/charmbracelet/glow) to render the output in the
terminal:

```bash
vast import documentation | glow -
```

Finally, the `manual` subcommand prints a man-page-like output for a given
command, plus all contained subcommands:

```bash
vast import manual | glow -
```

## Environment Variables

In addition to the command line, VAST offers environment variables as an
equivalent mechanism to provide options. This comes in handy when working with
non-interactive deployments where the command line is hard-coded, such as in
Docker containers.

An environment variable has the form `KEY=VALUE`, and we discuss the format of
`KEY` and `VALUE` below. VAST processes only environment variables having the
form `VAST_{KEY}=VALUE`. For example, `VAST_ENDPOINT=1.2.3.4` translates to the
the command line option `--endpoint=1.2.3.4` and YAML configuration
`vast.endpoint: 1.2.3.4`.

Regarding precedence, environment variables override configuration file
settings, and command line arguments override environment variables.

### Keys

There exists a one-to-one mapping from configuration file keys to environment
variable names. Here are two examples:

- `vast.import.batch-size` 👈 configuration file key
- `VAST_IMPORT__BATCH_SIZE` 👈 environment variable

A hierarchical key of the form `vast.x.y.z` maps to the environment variable
`VAST_X__Y__Z`. More generally, the `KEY` in `VAST_{KEY}=VALUE` adheres to the
following rules:

1. Double underscores map to the `.` separator of YAML dictionaries.

2. Single underscores `_` map to a `-` in the corresponding configuration file
   key. This is unambiguous because VAST does not have any options that include
   a literal underscore.

From the perspective of the command line, the environment variable key
`VAST_X__Y__Z` maps to `vast x y --z`. Here are two examples with identical
semantics:

```bash
VAST_IMPORT__BATCH_SIZE=42 vast import json < data
vast import --batch-size=42 json < data
```

:::caution CAF Settings
To provide CAF settings, which have the form `caf.x.y.z` in the configuration
file, the environment variable must have the form `VAST_CAF__X__Y__Z`.

The configuration file is an exception in this regard: `vast.caf.` is not a
valid key prefix. Instead, CAF configuration file keys have the prefix `caf.`,
i.e., they are hoisted into the global scope.
:::

### Values

While all environment variables are strings on the shell, VAST parses them into
a typed value internally. In general, parsing values from the environment
follows the same syntactical rules as command line parsing.

In particular, this applies to lists. For example, `VAST_PLUGINS="sigma,pcap"`
is equivalent to `--plugins=foo,bar`.

VAST ignores environment variables with an empty value because the type cannot
be inferred. For example, `VAST_PLUGINS=` will not be considered.

## Configuration files

VAST's configuration file is in YAML format. On startup, VAST attempts to read
configuration files from the following places, in order:

1. `<sysconfdir>/vast/vast.yaml` for system-wide configuration, where
   `sysconfdir` is the platform-specific directory for configuration files,
   e.g., `<install-prefix>/etc`.

2. `~/.config/vast/vast.yaml` for user-specific configuration. VAST respects
   the XDG base directory specification and its environment variables.

3. A path to a configuration file passed via `--config=/path/to/vast.yaml`.

If there exist configuration files in multiple locations, options from all
configuration files are merged in order, with the latter files receiving a
higher precedence than former ones. For lists, merging means concatenating the
list elements.

### Plugin Configuration Files

In addition to `vast/vast.yaml`, VAST loads `vast/plugin/<plugin>.yaml` for
plugin-specific configuration for a given plugin named `<plugin>`. The same
rules apply as for the regular configuration file directory lookup.

### Bare Mode

Sometimes, users may wish to run VAST without side effects, e.g., when wrapping
VAST in their own scripts. Run with `--bare-mode` to disable looking at all
system- and user-specified configuration paths.

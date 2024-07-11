---
sidebar_position: 2
---

# Modules

A **module** creates a namespace for [type
definitions#type-construction](type-system.md), [pipelines](../pipelines.md),
and [taxonomy](taxonomies.md) elements in single YAML file.

:::warning Work in Progress
While you can already [show schema
definitions](../user-guides/show-available-schemas.md), it is unfortunately not
yet possible write them in the same syntax. We are working towards closing this
gap, as tracked by [the corresponding roadmap
item](https://github.com/tenzir/public-roadmap/issues/15).
:::

## Type Definition Rules

All defined type names and aliases share one *global* identifier namespace.
Introducing a new type definition or alias adds a symbol to this namespace. The
following rules exist to make manipulation of the namespace manageable:

- Tenzir processes all directories of the `tenzir.module-dirs` option *in
  order*, creating a union of all type definitions.

- *Within* a specified module directory, all type definitions must be unique,
  i.e., no types can have the same name.

- *Across* directories, later definitions can override existing ones from
  previous directories. This allows users to adapt existing types by providing
  an alternate definition in a separate module directory.

- Resolving aliases to custom types follows a 2-phase lookup, which makes it
  possible to use a custom type and define it afterwards in the module file.
  The 2-phase lookup only works within a module directory.

:::note
Tenzir processes all directories *recursively*. This means you are free to split
the content over a directory structure of your choice.
:::

## Module Directory Lookup

Tenzir ships with modules containing type definitions and aliases for common
formats, such as Zeek or Suricata logs. Pre-installed modules reside in
`<datadir>/tenzir/modules`, and additional search paths for user-provided
modules can be set in the configuration file `tenzir.yaml` by adjusting the
`tenzir.module-dirs` option.

Tenzir looks at module directories in the following order:

1. `<datadir>/tenzir/module` for system-wide module files bundled with Tenzir,
   where `<datadir>` is the platform-specific directory for data files, e.g.,
   `<install-prefix>/share`.

2. `<sysconfdir>/tenzir/modules` for system-wide configuration, where
   `<sysconfdir>` is the platform-specific directory for configuration files,
   e.g., `<install-prefix>/etc`.

3. `~/.config/tenzir/modules` for user-specific configuration. Tenzir respects
   the XDG base directory specification and its environment variables.

4. An ordered, comma-separated list of directories passed using
   `--module-dirs=path/to/modules` on the command line. This corresponds to the
   option `tenzir.module-dirs`.

:::caution Changing Tenzir modules
We recommend to avoid making changes to module files in
`<datadir>/tenzir/modules`, as this can break updates to Tenzir. If you need to
make adaptations of builtin types, you can modify them in your own module
directory with the help of record type operations.
:::

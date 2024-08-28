---
sidebar_custom_props:
  operator:
    source: true
    transformation: false
    sink: false
---

# packages

Shows the installed packages.

## Synopsis

```
packages [--format=<format>]
```

## Description

The `packages` operator lists all installed packages.

### `<format>`

This option controls the output format of the `packages` operator.
Valid options are `compact` (the default) and `extended`. See the
`Package List Formats` section below for more details.

## Examples

Show a list of installed packages in compact format:

```
packages
```

Output:
```
{
  "id": "feodo",
  "name": "Feodo Abuse Blocklist",
  "author": "Tenzir",
  "description": "Feodo Tracker is a project of abuse.ch with the goal of sharing botnet C&C\nservers associated with Dridex, Emotet (aka Heodo), TrickBot, QakBot (aka\nQuakBot / Qbot) and BazarLoader (aka BazarBackdoor). It offers various\nblocklists, helping network owners to protect their users from Dridex and\nEmotet/Heodo.\n",
  "config": {
    "inputs": {},
    "overrides": {}
  }
}
```

## Package List Formats

The output of the `packages` operator can be controlled by the `--format`
parameter. There are currently two formats defined, the compact format
intended for human operators and the extended format intended for use by
shell scripts and other programs.

### Compact Format

The compact format prints the package information according to the following
schema:

|Field|Type|Description|
|:-|:-|:-|
|`id`|`string`|The unique package id.|
|`name`|`string`|The name of this package.|
|`author`|`string`|The package author.|
|`description`|`string`|The description of this package.|
|`config`|`record`|The user-provided package configuration|

### Extended Format

The `extended` format is mainly intended for use by non-human consumers,
like shell scripts or frontend code. It contains all available information
about a package.

|Field|Type|Description|
|:-|:-|:-|
|`package_definition`|`record`|The original package definition object asa found in the library.|
|`resolved_package`|`record`|The effective package definition that was produced by applying all inputs and overrides from the `config` section and removing all disabled pipelines and contexts.|
|`config`|`record`|The user-provided package configuration.|
|`package_status`|`record`|Run-time information about the package provided the package manager.|

The `config` object has the following schema, where all fields are optional:

|Field|Type|Description|
|:-|:-|:-|
|`version`|`string`|The package version.|
|`source`|`record`|The upstream location of the package definition.|
|`inputs`|`record`|User-provided values for the package inputs.|
|`overrides`|`record`|User-provided overrides for fields in the package definition.|
|`metadata`|`record`|An opaque record that can be set during installation.|

The `package_status` object has the following schema:

|Field|Type|Description|
|:-|:-|:-|
|`install_state`|`string`|The install state of this package. One of `installing`, `installed`, `removing` or `zombie`.|
|`from_configuration`|`bool`|Whether the package was installed from the `package add` operator or from a configuration file.|

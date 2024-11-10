# list

Shows installed packages.

```tql
package::list [format=string]
```

## Description

The `package::list` operator returns the list of all installed packages.

### `format = string (optional)`

Controls the output format. Valid options are `compact` and `extended`.

Defaults to `compact`.

## Schemas

The `package::list` operator produces two output formats, controlled by the
`format` option:

- `compact`: succinct output in a human-readable format
- `extended`: verbose output in a machine-readable format

The formats generate the following schemas below.

### `tenzir.package.compact`

The compact format prints the package information according to the following
schema:

|Field|Type|Description|
|:-|:-|:-|
|`id`|`string`|The unique package id.|
|`name`|`string`|The name of this package.|
|`author`|`string`|The package author.|
|`description`|`string`|The description of this package.|
|`config`|`record`|The user-provided package configuration|

### `tenzir.package.extended`

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

## Examples

### Show all installed packages

```tql
package::list
```

```tql
{
  "id": "suricata-ocsf",
  "name": "Suricata OCSF Mappings",
  "author": "Tenzir",
  "description": "[Suricata](https://suricata.io) is an open-source network monitor and\nthreat detection tool.\n\nThis package converts all Suricata events published on the topic `suricata` to\nOCSF and publishes the converted events on the topic `ocsf`.\n",
  "config": {
    "inputs": {},
    "overrides": {}
  }
}
```

## See Also

[`package::add`](add.md), [`package::remove`](remove.md)

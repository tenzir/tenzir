---
sidebar_custom_props:
  operator:
    source: true
---

# version

Shows the current version.

## Synopsis

```
version
```

## Description

The `version` operator shows the current Tenzir version.

## Schemas

Tenzir emits version information with the following schema.

### `tenzir.version`

Contains detailed information about the process version.

|Field|Type|Description|
|:-|:-|:-|
|`version`|`string`|The formatted version string.|
|`tag`|`string`|An optional identifier of the build.|
|`major`|`uint64`|The major release version.|
|`minor`|`uint64`|The minor release version.|
|`patch`|`uint64`|The patch release version.|
|`features`|`list<string>`|A list of feature flags that conditionally enable features in the Tenzir Platform.|
|`build`|`record`|Build-time configuration options.|
|`dependencies`|`list<record>`|A list of build-time dependencies and their versions.|

The `build` record contains the following fields:

|Field|Type|Description|
|:-|:-|:-|
|`type`|`string`|The configured build type. One of `Release`, `Debug`, or `RelWithDebInfo`.|
|`tree_hash`|`string`|A hash of all files in the source directory.|
|`assertions`|`bool`|Whether potentially expensive run-time checks are enabled.|
|`sanitizers`|`record`|Contains information about additional run-time checks from sanitizers.|

The `build.sanitzers` record contains the following fields:

|Field|Type|Description|
|:-|:-|:-|
|`address`|`bool`|Whether the address sanitizer is enabled.|
|`undefined_behavior`|`bool`|Whether the undefined behavior sanitizer is enabled.|

The `dependencies` record contains the following fields:

|Field|Type|Description|
|:-|:-|:-|
|`name`|`string`|The name of the dependency.|
|`version`|`string`|THe version of the dependency.|

## Examples

Use `version` to show the current version of a development build:

```
{
  "version": "v4.6.3-36-gbd4c8a058b-dirty",
  "major": 4,
  "minor": 6,
  "patch": 3,
  "tweak": 36
}
```

Use `version` to show the current version of a release build:

```
{
  "version": "v4.7.0",
  "major": 4,
  "minor": 7,
  "patch": 0,
  "tweak": 0
}
```

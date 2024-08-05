---
sidebar_custom_props:
  operator:
    source: true
---

# build

Shows build information.

## Synopsis

```
build
```

## Description

The `build` operator shows information about the build of the `tenzir` and
`tenzir-node` binaries.

## Schemas

Tenzir emits build information with the following schema.

### `tenzir.build`

Contains detailed information about the build.

|Field|Type|Description|
|:-|:-|:-|
|`version`|`string`|The formatted version string.|
|`type`|`string`|The configured build type. One of `Release`, `Debug`, or `RelWithDebInfo`.| 
|`tree_hash`|`string`|A hash of all files in the source directory.|
|`assertions`|`bool`|Whether potentially expensive run-time checks are enabled.|
|`sanitizers`|`record`|Contains information about additional run-time checks from sanitizers.|
|`features`|`list<string>`|A list of feature flags that conditionally enable features in the Tenzir Platform.|
|`dependencies`|`list<record>`|A list of build-time dependencies and their versions.|

The `sanitzers` record contains the following fields:

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

Get a list of build-time dependencies and their respective versions:

```
build
| yield dependencies[]
```

# version

Shows the current version.

## Synopsis

```
version
```

## Description

The `version` operator shows the current Tenzir version.

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

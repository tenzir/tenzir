---
title: "User defined operators in packages"
type: feature
authors: tobim
pr: 5496
---

This extends the package format with user defined operators.
A packaged operator can be used from a pipeline after the package is installed on a node.
Package operators are defined in `.tql` files the `operators` subdirectory of a package.
Once installed, the operators can be called by its ID, which is constructed from the filesystem path.
Here is an example from a hypothetical MISP package:
This is the directory structure with an operator:
```
└── misp
    └── operators
        └── event
            └── to_ocsf.tql
```
And you can use the operator in TQL:
```dart
misp::event::to_ocsf
```

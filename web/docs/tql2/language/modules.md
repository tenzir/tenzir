# Modules

TQL features namespacing in the form of *modules* to group related
[operators](../operators.md) and [functions](../functions.md).

A *module* separates the contained operation by two colons. For example, the
[`package::add`](../operators/package/add.md) and
[`package::remove`](../operators/package/remove.md) operators are in the
`package` module.

Modules are not hierarchical and have exactly one level of nesting. That is,
the language does not support constructs of the form `a::b::c`.

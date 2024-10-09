---
sidebar_position: 0
---

# Type System

One [design goal](../architecture/design-goals.md) of Tenzir is being expressive
enough to capture the semantics of the domain. This led us to develop a rich
type system for structured security data, inspired by the
[Zeek](https://zeek.org) network security monitor.

## Terminology

The diagram below illustrates the type system at a glance:

![Type System - Tenzir](type-system-tenzir.excalidraw.svg)

### Basic Types

Basic types are stateless types with a static structure. Their representation
is known a-priori.

The following tables summarizes the basic types.

| Type       | Description
| ---------- | --------------------------------------
| `none`     | Denotes an absent or invalid value
| `bool`     | A boolean value
| `int64`    | A 64-bit signed integer
| `uint64`   | A 64-bit unsigned integer
| `double`   | A 64-bit double (IEEE 754)
| `duration` | A time span (nanosecond granularity)
| `time`     | A time point (nanosecond granularity)
| `string`   | A UTF-8 encoded string
| `blob`     | An arbitrary sequence of bytes
| `pattern`  | A regular expression
| `ip`       | An IPv4 or IPv6 address
| `subnet`   | An IPv4 or IPv6 subnet

:::warning Experimental
The `blob` type is still experimental and not yet fully supported.
:::

### Complex Types

Complex types are stateful types that carry additional runtime information.

#### Enumeration

The `enum` type is a list of predefined string values. It comes in handy for
low-cardinality values from a fixed set of options.

Tenzir implements an `enum` as an Arrow Dictionary.

#### List

The `list` type is an ordered sequence of values with a fixed element type.

Lists have zero or more elements.

#### Record

The `record` type consists of an ordered sequence *fields*, each of which have a
name and type. Records must have at least one field.

The field name is an arbitrary UTF-8 string.

The field type is any Tenzir type.

### Optionality

All types are optional in that there exists an additional `null` data point in
every value domain. Consequently, Tenzir does not have a special type to
indicate optionality.

### Attributes

Every type has zero or more **attributes**, which are free-form key-value pairs
to enrich types with custom semantics.

For duration types, the attribute `unit` can be set to customize how numeric
values are interpreted by parsers. For example, `#unit=s` makes it so that the
number `1708430755` is parsed as `"1708430755s"`. The `unit` attribute can also
be set for timestamps, which means that numeric values are interpreted as
offsets from the Unix epoch. For example, `#unit=s` makes it so that
`1708430755` is parsed as `"2023-02-20T12:05:55"`.

### Aliases

An alias wraps an existing type under a new name. Aliases are first-class types,
meaning you can also attach separate attributes to them.

All alias types have a name. They coexist in a global namespace.

:::tip Alias = Specialization
An alias always *refines* the type it points to, i.e., it is more specific that
its parent. For example, let's assume a type `U` is an alias for an existing
type `T`. Then, the [type
extractor](../language/expressions.md#type-extractor) `:U` only
resolves for types of instance `U`. However, `:T` comprises both instances of
types `U` and `T`.
:::

## Comparison to Arrow

All Tenzir types have a lossless mapping to Arrow types, however, not all Arrow
types have a Tenzir equivalent. As a result, it is currently not yet possible to
import arbitrary Arrow data. In the future, we plan to extend our support for
Arrow-native types and also offer conversion options for seamless data handover.

Tenzir has a few domain-specific types that map to Arrow [extension
types][extension-types]. These are currently:

- `enum`
- `ip`
- `subnet`

[extension-types]: https://arrow.apache.org/docs/format/Columnar.html#extension-types

In Tenzir, the type is the entity that contains metadata. In Arrow, it's the
record field or the schema.

:::tip More on Arrow & Tenzir
If you want to learn more about why Tenzir uses Apache Arrow, please read our
[two](/blog/apache-arrow-as-platform-for-security-data-engineering) [blog
posts](/blog/parquet-and-feather-enabling-open-investigations) that explain why
we build on top of Arrow.
:::

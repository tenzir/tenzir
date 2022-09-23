---
sidebar_position: 0
---

# Type System

One [design goal](/docs/understand/architecture/design-goals) of VAST is
being expressive enough to capture the semantics of the domain. This led us to
develop a rich type system for structured security data, inspired by the data
model of the [Zeek](https://zeek.org) network security monitor.

Every type has zero or more **attributes**, which are free-form key-value pairs
to enrich types with custom semantics. Fundamentally, types support
**aliasing**, which means you can give an existing type a new name. All types,
including aliases, share a global identifier namespace. In this namespace, type
names must be unique.

There exist two major classes of types:

1. **Basic**: stateless types with a static structure and a-priori known representation
2. **Complex**: stateful types that carry additional runtime information

The diagram below illustrates the type system:

![Type System](/img/type-system-vast.light.png#gh-light-mode-only)
![Type System](/img/type-system-vast.dark.png#gh-dark-mode-only)

There exists a one-to-one mapping of VAST's type system to Arrow's type system
to ensure that transferred data is self-describing:

![Type System](/img/type-system-arrow.light.png#gh-light-mode-only)
![Type System](/img/type-system-arrow.dark.png#gh-dark-mode-only)

Note that VAST attaches attributes to a top-plevel type instance, where Arrow
only allows type meta data for record fields. VAST converts enum, adress, and
subnet types to [extension-types][extension-types].

[extension-types]: https://arrow.apache.org/docs/format/Columnar.html#extension-types

:::tip More on Arrow & VAST
If you want to learn more about why VAST uses Apache Arrow, please read [our
blog post](/blog/apache-arrow-as-platform-for-security-data-engineering) that
explains why we use Arrow as corner stone for data interoperability and
representation of the security domain model.
:::

---
sidebar_position: 1
---

# Schemas

VAST defines a **schema** as a *named* record type. Since all types can have
a name, we define a schema as a type a that (1) is a record, and (2) has a name.
From a data perspective, the schema specifies the structure of a batch of
records.

Ideally, each data source defines its own semantically rich schema to retain
most of the domain-specific information of the data. This is desirable because
accurately modeled data is more productive to work with because it's less
error-prone to misinterpret and requires fewer context switches to infer missing
gaps. [VAST's type system](type-system) is well-suited for deep domain modeling:
it can express structure with lists and records, add meta data to any types via
tags, and also support aliasing for building libraries of composable types.

In practice, many tools often "dumb down" their rich internal representation
into a generic piece of JSON, CSV, or text. This puts the burden of gaining
actionable insights onto the user downstream: either they work with a minimal
layer of typing, or they put in effort to (re)apply a coat of typing by writing
a schema.

However, writing and managing schemas can quickly escalate: they evolve
continuously and induce required changes in downstream analytics. VAST aims to
minimize the needed effort to maintain schemas by tracking their lineage, and by
making data sources infer a basic schema that serves as reasonable starting
point. For example, the [JSON](../formats/json.md) reader attempts to
parse strings as timestamps, IP address, or subnets, to gather a deeper semantic
meaning than "just a string." The idea is to make it easy to get started but
still allow for later refinements. You would provide a schema when you would
like to boost the semantics of your data, e.g., to imbue meaning into generic
string values by creating an alias type, or to enrich types with free-form
attributes.

:::note Why factor types?
Many data sources emit more than one event in the form of a record, and often
contain nested records shared across multiple event types. For example, the
majority of [Zeek](../formats/zeek.md) logs have the connection
record in common. Factoring this shared record into its own type, and then
reusing across all other occurrences makes it easy to perform cross-event
connection analysis later on.
:::

You can write schemas manually by providing a [module](modules.md). To find out
existing schemas, consult the documentation on
[introspection](../../use/introspect/README.md).

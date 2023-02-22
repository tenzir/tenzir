# Schemas

A schema is the specification of a record type for a batch of data. Within a
batch of records, all records share the same schema. VAST embeds the schema as
metadata in the form of a [FlatBuffer](https://google.github.io/flatbuffers/) in
every batch.

Ideally, each data source defines its own semantically rich schema to retain
most of the domain-specific information of the data. This is desirable because
accurately modeled data is more productive to work with because it's less
error-prone to misinterpret and requires fewer context switches to infer missing
gaps. [VAST's type system](/docs/understand/data-model/type-system) is
well-suited for deep domain modeling: it can express structure with lists and
records, add meta data to any types via tags, and also support aliasing for
building libraries of composable types.

In practice, many tools often "dumb down" their rich internal representation
into a generic piece of JSON, CSV, or text. This puts the burden of gaining
actionable insights onto the analyst downstream: either they work with a minimal
layer of typing, or they put in effort to (re)apply a coat of typing by writing
a schema.

However, writing and managing schemas can quickly escalate: they evolve
continuously and induce required changes in downstream analytics. VAST aims to
minimize the needed effort to maintain schemas by tracking their lineage, and by
making data sources infer a basic schema that serves as reasonable starting
point. For example, the [JSON](formats/json) reader attempts to parse strings as
timestamps, IP address, or subnets, to gather a deeper semantic meaning than
"just a string." The idea is to make it easy to get started but still allow for
later refinements. You would provide a schema when you would like to boost the
semantics of your data, e.g., to imbue meaining into generic string values by
creating an alias type, or to enrich types with free-form attributes.

:::note Why factor types?
Many data sources emit more than one event in the form of a record, and often
contain nested records shared across multiple event types. For example, the
majority of [Zeek](/docs/understand/formats/zeek) logs have the connection
record in common. Factoring this shared record into its own type, and then
reusing across all other occurences makes it easy to perform cross-event
connection analysis later on.
:::

## Schema Definition

In VAST, a **schema** is a *named* record type. In fact, all types have an
optional name, not just records. What makes a type a schema is that (1) it is a
record, and (2) that it has a name.

From the perspective of naming, VAST distinguishs two kinds of types:

1. **Anonymous types**: types *without* a name
2. **Alias types**: types *with* a name

## Type Construction

VAST comes with a lean type definition language. Think [JSON
schema](https://json-schema.org/) fused with [Kaitai](https://kaitai.io/), with
the goal that humans can jot it down quickly. We use YAML as vehicle to express
the structure because that's already VAST's language for configuration.

How do you refer to a type? The basic scaffold looks like this:

```yaml
type: T
attributes:
  - key: value
  - value # values are optional
```

This assumes that the type `T` exists already. This begs a recursive question:
what are valid values for `T`? These are either basic types, such as `bool`,
`string`, `ip`, or types defined according the rules described below.

### Alias

An alias defines a new name for a type, allowing for reuse across other types.
For example:

```yaml
url: string
```

This introduces a type with the name `url`.

:::caution Basic types & aliases
The type `url` is different from `string` because it has a different name. Basic
types do not have a name, i.e., they are anonymous types.

TODO: This needs a better explanation.
:::

Instead of referencing an existing type, you can also provide an inline
definition of a type:

```yaml
url:
  type: string
  attributes:
    - unescaped
```

### Enumeration

An enumeration is a list of predefined string values. Use an `enum` dictionary
with a list of allowed values to define it:

```yaml
enum:
  - on
  - off
```

### List

Use the `list` key to define list with a the corresponding element type on the
right-hand side:

```yaml
list: url
```

Lists nest naturally:

```yaml
list:
  list:
    enum:
      - on
      - off
```

### Record

Use the `record` key to define a list of fields:

```yaml
record:
  - src_ip: ip
  - dst_ip: ip
  - src_port: port # existing alias
  - dst_port: port # existing alias
  - proto: string
```

Records nest naturally:

```yaml
record:
  - src: 
      record:
        - ip: ip
        - port: uint64
  - dst: 
      record:
        - ip: ip
        - port: uint64
  - proto: string
```

Same as above, but with the nested records factored as type aliases:

```yaml
endpoint:
  record:
    - ip: ip
    - port: uint64

connection:
  record:
    - src: endpoint
    - dst: endpoint
```

Records of lists:

```yaml
record:
  - values:
      list: string
```

Lists of records:

```yaml
list:
  record:
    - question: string
    - correct: bool
```

#### Record Algebra

In addition to nesting records, a frequent use case is embedding fields of
one record into another. This occurs often with families of event types that
share a large portion of fields. Algebraically, we want to "add" the two records
together to combine their field.

For example, consider this `common` record with a single field `shared`:

```yaml
# The base record.
common:
  record:
    - shared:
        type: bool
```

To embed `common` in another record, reference it using the `base` key. This
requires moving the fields one layer down under the `fields` key:

```yaml
record:
  base:
    - common
  fields:
    - unique: string
```

The effect is the same specifying a single record with two fields:

```yaml
record:
  - shared: bool
  - unique: string
```

There are three special keys under the `record` key that control what to do when
field name clashes occur between the two records:

1. `base`: raise an error
2. `implant`: prefer the base record
3. `extend`: prefer the current record

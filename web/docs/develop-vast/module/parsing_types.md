# Parsing YAML Types

## Type parsing redesign goals

1. Improve error handling when parsing.
2. Easy way to bundle type, concept, and model definitions.
3. Make dumping types in commands like `vast dump` and `vast status` easy, and
   use YAML only as output format.

## New way of parsing types

We are using a minimal YAML-based layout that reads similar to the old DSL. The
key property is that there are many places where a type can occur, and in all
places, it is valid to put all variations of type.

In the following, the essential rule to remember is that a type is a YAML
dictionary; only in type alias and record field declaration can it be a YAML
string value for brevity.

## Type Alias

Define a new type alias as YAML dictionary, with a type key:

```
url:
  type: string
```

Every type can have metadata in the form of attributes, which are free-form
key-values pairs with an optional value:

```
url:
  type: string
  attributes:
  - index: hash
  - ioc
```

# List

A list type has the list key and a valid type on the RHS:

```
urls:
  list: url
```

# Map

A map type is a YAML dictionary with a key and a value, each of which references
an existing type:

```
urls:
  map:
    key: string
    value: bool
```

As with record fields and other types, inline type definitions also work.

# Record

A record type is a list of singleton YAML dictionaries, each of which represents
a field:

```
connection:
  record:
  - src_ip: addr
  - dst_ip: addr
  - src_port: port
  - dst_port: port
  - proto: string
```

The RHS of a field definition is either a type identifier 👆 or an inline type
definition 👇:

```
flow:
  record:
  - source:
      type: addr
      attributes:
      - originator
  - destination:
      type: addr
      attributes:
      - responder
```

Record of lists are possible:

```
matrix:
  record:
  - values:
      list:
        list:
          real
```

Likewise, lists of records:

```
answers:
  list:
    record:
    - question: string
    - correct: bool
```

# Enumeration

An enum type has the enum key and contains a list of strings:

```
status:
  enum:
  - on
  - off
  - unknown
```

# Record Algebra

When composing records, we need need to define records in their extended form,
which requires an additional `fields` key:

```
# The base record.
common:
  record:
  - field:
      type: bool

# The combined record...
bundle:
  record:
    base:
      - common
    fields:
      - msg: string

# ...which has the same layout as this specification:
bundle:
- field: bool
- msg: string
```

There are three special keys under the record key that control what to do when a
field name clash occurs:

1. `base`: raise an error
2. `implant`: prefer the base record
3. `extend`: prefer the current record

## Inlining and naming

The type declaration syntax does not provide a way to name inline types, which
means that only top-level types can have a name.

If multiple declarations use the same inline type, it may be worth to use YAML
node aliases. Example:

```yaml
# 1. The same inline type is used multiple times:

type1:
  list:
    record:
      - src: addr
      - dst: addr

type2:
  map:
    key: string
    value:
      record:
        - src: addr
        - dst: addr
```

```yaml
# 2. Using a YAML node alias:

type1:
  list: &record1
    record:
    - src: addr
    - dst: addr

type2:
  map:
    key: string
    value: *record1
```

Type1 and type2 in the first example are equivalent to type1 and type2 in the
second example.

There are no restrictions on declaration names; if the YAML parser can handle a
name, then it is a valid name. To prevent confusion the only rule is that a
declaration name cannot be a VAST type name: `bool`, `integer`, `count`, `real`,
`duration`, `time`, `string`, `pattern`, `addr`, `subnet`, `enum`, `list`,
`map`, `record`.

## Type Alias

It is possible to declare a Type Alias referencing a built-in simple type, or
another declared type. Examples:

```yaml
type1:
   type: type2

 type2:
   type: count
```

A Type Alias cannot have an inline type. Rationale:

```yaml
type1:
  type:
    type: addr

# Type1 is euqivalent to the more straightforward:

type1:
  type: addr
```

```yaml
type2:
  type:
    list: addr

# Type2 is equivalent to the simpler:

type2:
  list: addr
```

## Record Algebra

A Record Algebra is not a VAST type; it defines operations on record types. The
module parser represents the unresolved YAML declaration as a VAST type; this
makes parsing a Record Algebra challenging.

The module parser does not support inlining Record Algebras; inlining leads to a
complex user experience as the examples show.

Inlined Record Algebra would look like:

```yaml
list1:
  list:
    record:
      base:
        - record1
      fields:
        - new_field: string
```

```yaml
map1:
  map:
    key: string
    value:
      record:
        base:
          - record1
        fields:
          - new_field: string
```

```yaml
record_algebra1:
  record:
    base:
      - record2
    fields:
      - record_algebra_field:
          record:
            base:
              - record1
            fields:
              - new_field: string
```

## Attributes

Every type can have attributes:

```yaml
type_alias:
  type: string
  attributes:
    - attr1_key: attr1_value
    - attr2_value:
    - attr3_value
```

```yaml
enum_type:
  enum:
    - on
    - off
    - unknown
  attributes:
    - attr_value
```

```yaml
list_type:
  list: url
  attributes:
    - attr_value
```

```yaml
map_type:
  map:
    key: string
    value: bool
  attributes:
    - attr_value
```

```yaml
record_type:
  record:
    - field1: addr
  attributes:
    - attr_value
```

```yaml
record_algebra_type:
  record:
    base:
      - record1
    fields:
      - field1: string
    attributes:
      - attr_value
```

# Parsing YAML Types
## Inlining and naming

The type declaration syntax does not provide a way to name inline types, which
means that only top-level types can have a name.

If multiple declarations use the same inline type, it is worth declaring the
inline type at the top level, giving it a name and referring to it by that
name. Example:

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
# 2. Declare the type used multiple times at the top level instead of inlining:

type1:
  list: record_type

type2:
  map:
    key: string
    value: record_type

record_type:
  record:
    - src: addr
    - dst: addr

```

Type1 and type2 in the first example are equivalent to type1 and type2 in the
second example.

There are no restrictions on declaration names; if the YAML parser can handle a
name, then it is a valid name. To prevent confusion the only rule is that a
declaration name cannot be a VAST type name: `bool`, `integer`, `count`, `real`,
`duration`, `time`, `string`, `pattern`, `addr`, `subnet`, `enum`, `list`,
`map`, `record`. There is no equivalent for the VAST none type in a YAML Module,
so `none` is not a reserved name.

## Type Alias

It is possible to declare a Type Alias referencing a built-in simple type, or
another declared type. Examples:

```yaml
type1:
   type: type2

 type2:
   type: count
```

A Type Alias cannot have an inline type. Examples:

```yaml
type1:
  type:
    type: addr

# Type1 is more straightforward as:

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

Inlined Record Algebra would look like these

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

The examples show that reading inline Record Algebras may not be an easy user
experience. For these reasons, the module parser does not support inlining
Record Algebras.

## Attributes

Every type can have attributes:

```yaml
type_alias:
  type: string
  attributes:
    - attr1_key: attr1_value
    - attr2_value
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

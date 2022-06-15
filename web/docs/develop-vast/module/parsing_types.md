# Parsing YAML Types
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

# Module Syntax

The syntax below is designed to produce a YAML oneliner which can be placed
inside the `types` dictionary of a YAML Module.

## Type-declaration

![Type-declaration Railroad Diagram](/img/module/Type-declaration.light.svg#gh-light-mode-only)
![Type-declaration Railroad Diagram](/img/module/Type-declaration.dark.svg#gh-dark-mode-only)

```
Type-declaration
         ::= Type-alias
           | List
           | Map
           | Record
           | Enumeration
           | Record-algebra
```

no references

## Type-alias

![Type-alias Railroad Diagram](/img/module/Type-alias.light.svg#gh-light-mode-only)
![Type-alias Railroad Diagram](/img/module/Type-alias.dark.svg#gh-dark-mode-only)

```
Type-alias
         ::= '{' Declaration-name ':' ( Type-name | Inline-type-alias ) '}'
```

referenced by

- [Type-declaration](#type-declaration)

## Inline-type-alias

![Inline-type-alias Railroad Diagram](/img/module/Inline-type-alias.light.svg#gh-light-mode-only)
![Inline-type-alias Railroad Diagram](/img/module/Inline-type-alias.dark.svg#gh-dark-mode-only)

```
Inline-type-alias
         ::= '{' 'type' ':' Type-name Optional-attributes '}'
```

referenced by

- [Inline-type](#inline-type)
- [Type-alias](#type-alias)

## List

![List Railroad Diagram](/img/module/List.light.svg#gh-light-mode-only)
![List Railroad Diagram](/img/module/List.dark.svg#gh-dark-mode-only)

```
List     ::= '{' Declaration-name ':' Inline-list '}'
```

referenced by

- [Type-declaration](#type-declaration)

## Inline-list

![Inline-list Railroad Diagram](/img/module/Inline-list.light.svg#gh-light-mode-only)
![Inline-list Railroad Diagram](/img/module/Inline-list.dark.svg#gh-dark-mode-only)

```
Inline-list
         ::= '{' 'list' ':' Type-name-or-inline Optional-attributes '}'
```

referenced by

- [Inline-type](#inline-type)
- [List](#list)

## Map

![Map Railroad Diagram](/img/module/Map.light.svg#gh-light-mode-only)
![Map Railroad Diagram](/img/module/Map.dark.svg#gh-dark-mode-only)

```
Map      ::= '{' Declaration-name ':' Inline-map '}'
```

referenced by

- [Type-declaration](#type-declaration)

## Inline-map

![Inline-map Railroad Diagram](/img/module/Inline-map.light.svg#gh-light-mode-only)
![Inline-map Railroad Diagram](/img/module/Inline-map.dark.svg#gh-dark-mode-only)

```
Inline-map
         ::= '{' 'map' ':' '{' Inline-map-key ',' Inline-map-value '}' Optional-attributes '}'
```

referenced by

- [Inline-type](#inline-type)
- [Map](#map)

## Inline-map-key

![Inline-map-key Railroad Diagram](/img/module/Inline-map-key.light.svg#gh-light-mode-only)
![Inline-map-key Railroad Diagram](/img/module/Inline-map-key.dark.svg#gh-dark-mode-only)

```
Inline-map-key
         ::= 'key' ':' Type-name-or-inline
```

referenced by

- [Inline-map](#inline-map)

## Inline-map-value

![Inline-map-value Railroad Diagram](/img/module/Inline-map-value.light.svg#gh-light-mode-only)
![Inline-map-value Railroad Diagram](/img/module/Inline-map-value.dark.svg#gh-dark-mode-only)

```
Inline-map-value
         ::= 'value' ':' Type-name-or-inline
```

referenced by

- [Inline-map](#inline-map)

## Record

![Record Railroad Diagram](/img/module/Record.light.svg#gh-light-mode-only)
![Record Railroad Diagram](/img/module/Record.dark.svg#gh-dark-mode-only)

```
Record   ::= '{' Declaration-name ':' Inline-record '}'
```

referenced by

- [Type-declaration](#type-declaration)

## Inline-record

![Inline-record Railroad Diagram](/img/module/Inline-record.light.svg#gh-light-mode-only)
![Inline-record Railroad Diagram](/img/module/Inline-record.dark.svg#gh-dark-mode-only)

```
Inline-record
         ::= '{' 'record' ':' '[' ListRecord-field ']' Optional-attributes '}'
```
referenced by

- [Inline-type](#inline-type)
- [Record](#record)

## ListRecord-field

![ListRecord-field Railroad Diagram](/img/module/ListRecord-field.light.svg#gh-light-mode-only)
![ListRecord-field Railroad Diagram](/img/module/ListRecord-field.dark.svg#gh-dark-mode-only)

```
ListRecord-field
         ::= Record-field ( ',' Record-field )*
```

referenced by

- [Inline-record](#inline-record)
- [Record-declaration](#record-declaration)

## Record-field

![Record-field Railroad Diagram](/img/module/Record-field.light.svg#gh-light-mode-only)
![Record-field Railroad Diagram](/img/module/Record-field.dark.svg#gh-dark-mode-only)

```
Record-field
         ::= '{' Field-name ':' Type-name-or-inline '}'
```

referenced by

- [ListRecord-field](#listrecord-field)

## Enumeration

![Enumeration Railroad Diagram](/img/module/Enumeration.light.svg#gh-light-mode-only)
![Enumeration Railroad Diagram](/img/module/Enumeration.dark.svg#gh-dark-mode-only)

```
Enumeration
         ::= '{' Declaration-name ':' Inline-enumeration '}'
```

referenced by

- [Type-declaration](#type-declaration)

## Inline-enumeration

![Inline-enumeration Railroad Diagram](/img/module/Inline-enumeration.light.svg#gh-light-mode-only)
![Inline-enumeration Railroad Diagram](/img/module/Inline-enumeration.dark.svg#gh-dark-mode-only)

```
Inline-enumeration
         ::= '{' 'enum' ':' '[' Enum-value ( ',' Enum-value )* ']' Optional-attributes '}'
```

referenced by

- [Enumeration](#enumeration)

## Record-algebra

![Record-algebra Railroad Diagram](/img/module/Record-algebra.light.svg#gh-light-mode-only)
![Record-algebra Railroad Diagram](/img/module/Record-algebra.dark.svg#gh-dark-mode-only)

```
Record-algebra
         ::= '{' Declaration-name ':' Record-algebra-declaration '}'
```

referenced by

- [Type-declaration](#type-declaration)

## Record-algebra-declaration

![Record-algebra-declaration Railroad Diagram](/img/module/Record-algebra-declaration.light.svg#gh-light-mode-only)
![Record-algebra-declaration Railroad Diagram](/img/module/Record-algebra-declaration.dark.svg#gh-dark-mode-only)

```
Record-algebra-declaration
         ::= '{' 'record' ':' '{' Base-records-and-record '}' '}'
```
referenced by

- [Record-algebra](#record-algebra)

## Base-records-and-record

![Base-records-and-record Railroad Diagram](/img/module/Base-records-and-record.light.svg#gh-light-mode-only)
![Base-records-and-record Railroad Diagram](/img/module/Base-records-and-record.dark.svg#gh-dark-mode-only)

```
Base-records-and-record
         ::= Base-records-declaration ',' Record-declaration
```

referenced by

- [Record-algebra-declaration](#record-algebra-declaration)

## Base-records-declaration

![Base-records-declaration Railroad Diagram](/img/module/Base-records-declaration.light.svg#gh-light-mode-only)
![Base-records-declaration Railroad Diagram](/img/module/Base-records-declaration.dark.svg#gh-dark-mode-only)

```
Base-records-declaration
         ::= Name-clash-specifier ':' '[' Base-record-name ( ',' Base-record-name )* ']'
```
referenced by

- [Base-records-and-record](#base-records-and-record)

## Record-declaration

![Record-declaration Railroad Diagram](/img/module/Record-declaration.light.svg#gh-light-mode-only)
![Record-declaration Railroad Diagram](/img/module/Record-declaration.dark.svg#gh-dark-mode-only)

```
Record-declaration
         ::= 'fields' ':' '[' ListRecord-field ']' Optional-attributes
```

referenced by

- [Base-records-and-record](#base-records-and-record)

## Name-clash-specifier

![Name-clash-specifier Railroad Diagram](/img/module/Name-clash-specifier.light.svg#gh-light-mode-only)
![Name-clash-specifier Railroad Diagram](/img/module/Name-clash-specifier.dark.svg#gh-dark-mode-only)

```
Name-clash-specifier
         ::= 'base'
           | 'implant'
           | 'extend'
```

referenced by

- [Base-records-declaration](#base-records-declaration)

## Optional-attributes

![Optional-attributes Railroad Diagram](/img/module/Optional-attributes.light.svg#gh-light-mode-only)
![Optional-attributes Railroad Diagram](/img/module/Optional-attributes.dark.svg#gh-dark-mode-only)

```
Optional-attributes
         ::= ε
           | ',' 'attributes' ':' '[' Attribute ( ',' Attribute )* ']'
```

referenced by

- [Inline-enumeration](#inline-enumeration)
- [Inline-list](#inline-list)
- [Inline-map](#inline-map)
- [Inline-record](#inline-record)
- [Inline-type-alias](#inline-type-alias)
- [Record-declaration](#record-declaration)

## Attribute

![Attribute Railroad Diagram](/img/module/Attribute.light.svg#gh-light-mode-only)
![Attribute Railroad Diagram](/img/module/Attribute.dark.svg#gh-dark-mode-only)

```
Attribute
         ::= Attribute-key
           | '{' Attribute-key ':' Attribute-value? '}'
```

referenced by

- [Optional-attributes](#optional-attributes)

## Type-name

![Type-name Railroad Diagram](/img/module/Type-name.light.svg#gh-light-mode-only)
![Type-name Railroad Diagram](/img/module/Type-name.dark.svg#gh-dark-mode-only)

```
Type-name
         ::= Built-in-simple-type
           | Declaration-name
```

referenced by

- [Inline-type-alias](#inline-type-alias)
- [Type-alias](#type-alias)
- [Type-name-or-inline](#type-name-or-inline)

## Inline-type

![Inline-type Railroad Diagram](/img/module/Inline-type.light.svg#gh-light-mode-only)
![Inline-type Railroad Diagram](/img/module/Inline-type.dark.svg#gh-dark-mode-only)

```
Inline-type
         ::= Inline-type-alias
           | Inline-list
           | Inline-map
           | Inline-record
```

referenced by

- [Type-name-or-inline](#type-name-or-inline)

## Type-name-or-inline

![Type-name-or-inline Railroad Diagram](/img/module/Type-name-or-inline.light.svg#gh-light-mode-only)
![Type-name-or-inline Railroad Diagram](/img/module/Type-name-or-inline.dark.svg#gh-dark-mode-only)

```
Type-name-or-inline
         ::= Type-name
           | Inline-type
```

referenced by:

- [Inline-list](#inline-list)
- [Inline-map-key](#inline-map-key)
- [Inline-map-value](#inline-map-value)
- [Record-field](#record-field)

## Declaration-name

![Declaration-name Railroad Diagram](/img/module/Declaration-name.light.svg#gh-light-mode-only)
![Declaration-name Railroad Diagram](/img/module/Declaration-name.dark.svg#gh-dark-mode-only)

```
Declaration-name
         ::= 'Identifier'
```

referenced by

    Enumeration
    List
    Map
    Record
    Record-algebra
    Type-alias
    Type-name

## Field-name

![Field-name Railroad Diagram](/img/module/Field-name.light.svg#gh-light-mode-only)
![Field-name Railroad Diagram](/img/module/Field-name.dark.svg#gh-dark-mode-only)

```
Field-name
         ::= 'Identifier'
```
referenced by

    Record-field

## Enum-value

![Enum-value Railroad Diagram](/img/module/Enum-value.light.svg#gh-light-mode-only)
![Enum-value Railroad Diagram](/img/module/Enum-value.dark.svg#gh-dark-mode-only)

```
Enum-value
         ::= 'Identifier'
```

referenced by

    Inline-enumeration

## Base-record-name

![Base-record-name Railroad Diagram](/img/module/Base-record-name.light.svg#gh-light-mode-only)
![Base-record-name Railroad Diagram](/img/module/Base-record-name.dark.svg#gh-dark-mode-only)

```
Base-record-name
         ::= 'Identifier'
```

referenced by

    Base-records-declaration

## Attribute-key

![Attribute-key Railroad Diagram](/img/module/Attribute-key.light.svg#gh-light-mode-only)
![Attribute-key Railroad Diagram](/img/module/Attribute-key.dark.svg#gh-dark-mode-only)

```
Attribute-key
         ::= 'Identifier'
```

referenced by

    Attribute

## Attribute-value

![Attribute-value Railroad Diagram](/img/module/Attribute-value.light.svg#gh-light-mode-only)
![Attribute-value Railroad Diagram](/img/module/Attribute-value.dark.svg#gh-dark-mode-only)

```
Attribute-value
         ::= 'Identifier'
           | ε
```

referenced by

    Attribute

## Built-in-simple-type

![Built-in-simple-type Railroad Diagram](/img/module/Built-in-simple-type.light.svg#gh-light-mode-only)
![Built-in-simple-type Railroad Diagram](/img/module/Built-in-simple-type.dark.svg#gh-dark-mode-only)

```
Built-in-simple-type
         ::= bool
           | integer
           | count
           | real
           | duration
           | time
           | string
           | pattern
           | addr
           | subnet
```

referenced by

    Type-name

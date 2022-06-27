# Module Syntax

The syntax below is designed to produce a YAML oneliner which can be placed
inside the `types` dictionary of a YAML Module.

The railroad diagrams are generated
[generated](#Generating-the-railroad-diagrams) from Labelled BNF(LBNF) file.

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

## Generating the railroad diagrams

1. After installing `BNFC` and `pdflatex` run:

```
~/.cabal/bin/bnfc -m --latex types.cf
```

2. Edit `types.tex` add the following line after the line with `\documentclass`

```
\usepackage[margin=2cm,landscape,a3paper]{geometry}
```

3. Convert the tex file to pdf

```
pdflatex types.tex
```

4. Open pdf and save as `types.bnf`
   (with KDE's okular: File / Export As / Plain Text...)

```
okular types.pdf
```

5. Edit `types.bnf` remove everything so that only the `BNF` grammar remains.
   Page numbers has to be removed also.

6. Convert the BNF to W3C EBNF:

```
cat types.bnf | perl -pe 's/{/"{"/g' | perl -pe 's/}/"}"/g' | \
    perl -pe 's/ : / ":" /g' | perl -pe 's/ \[ / "[" /g' | \
    perl -pe 's/ \] / "]" /g'  | perl -pe 's/ \]/ "]"/g'  | perl -pe 's/⟨//g' | \
    perl -pe 's/ ⟩//g' | perl -pe 's/ ⟩//g' | perl -pe 's/,/","/g' | \
    perl -pe 's/  2//g' | \
    perl -pe 's/ attributes / "attributes" /g' | \
    perl -pe 's/ base/ "base"/g' | \
    perl -pe 's/ enum / "enum" /g' | \
    perl -pe 's/ extend/ "extend"/g' | \
    perl -pe 's/ fields / "fields" /g' | \
    perl -pe 's/ implant/ "implant"/g' | \
    perl -pe 's/ key / "key" /g' | \
    perl -pe 's/ list / "list" /g' | \
    perl -pe 's/ map / "map" /g' | \
    perl -pe 's/ record / "record" /g' | \
    perl -pe 's/ type / "type" /g' | \
    perl -pe 's/ value / "value" /g' | \
    perl -pe 's/Ident/ "Identifier" /g' > types.ebnf
```

7. Convert the W3C EBNF to railroad diagram (Edit Grammar and View Diagram
   tabs): https://www.bottlecaps.de/rr/ui

8. Make sure to uncheck the "Inline literals" when generating the diagram.

9. Save the railroad diagram as XHTML+SVG.

10. Extract the images out of the diaram with xsltproc. (This can take a couple
    of minutes)

```
xsltproc --param index 0 extract_svg.xsl diagram.xhtml > svgs.txt
```

The `extract_svg.xsl`:

```
<?xml version="1.0"?>

<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                version="1.0">
  <xsl:param name="index" />

 <xsl:template match="node()|@*">
   <xsl:apply-templates select="node()|@*"/>
 </xsl:template>

 <xsl:template match="//*[local-name()='svg']">
   <xsl:copy-of select="." />
 </xsl:template>

 <xsl:template match="//*[local-name()='div' and @class='ebnf']">
   --- CUT: <xsl:value-of select=".//*[local-name()='a' and local-name(parent::*)='div']/text()" /> ---
 </xsl:template>
</xsl:stylesheet>
```

11. Cut out the images and change the colors with the python script:

```
#!/usr/bin/env python3
import re

svgs = open('svgs.txt', 'r')
lines = svgs.readlines()
light_svg = ""
dark_svg = ""
for line in lines:
  if not line.startswith('   --- CUT: '):
    line = re.sub('<a[^>]*>', '', line)
    line = line.replace('</a>', '')
    light_line = line.replace(
      '.line                 {fill: none; stroke: #332900; stroke-width: 1;}',
      '.line                 {fill: none; stroke: #000000; stroke-width: 2;}')
    light_line = light_line.replace(
      '{stroke: #141000; shape-rendering: crispEdges; stroke-width: 2;}',
      '{stroke: #141000; shape-rendering: crispEdges; stroke-width: 4;}')
    light_line = light_line.replace(
      'polygon {fill: #332900; stroke: #332900;}',
      'polygon {fill: #000000; stroke: #332900;}')
    light_line = light_line.replace(
      '.terminal         {fill: #FFDB4D; stroke: #332900; stroke-width: 1;}',
      '.terminal         {fill: #00A4F1; stroke: #332900; stroke-width: 1;}')
    light_line = light_line.replace(
      '.nonterminal      {fill: #FFEC9E; stroke: #332900; stroke-width: 1;}',
      '.nonterminal      {fill: #00EDE1; stroke: #332900; stroke-width: 1;}')
    light_svg += light_line
    dark_line = line.replace(
      '.line                 {fill: none; stroke: #332900; stroke-width: 1;}',
      '.line                 {fill: none; stroke: #FFFFFF; stroke-width: 2;}')
    dark_line = dark_line.replace(
      '{stroke: #141000; shape-rendering: crispEdges; stroke-width: 2;}',
      '{stroke: #FFFFFF; shape-rendering: crispEdges; stroke-width: 4;}')
    dark_line = dark_line.replace(
      'polygon {fill: #332900; stroke: #332900;}',
      'polygon {fill: #FFFFFF; stroke: #332900;}')
    dark_line = dark_line.replace(
      '.terminal         {fill: #FFDB4D; stroke: #332900; stroke-width: 1;}',
      '.terminal         {fill: #00A4F1; stroke: #332900; stroke-width: 1;}')
    dark_line = dark_line.replace(
      '.nonterminal      {fill: #FFEC9E; stroke: #332900; stroke-width: 1;}',
      '.nonterminal      {fill: #00EDE1; stroke: #332900; stroke-width: 1;}')
    dark_svg += dark_line
    continue
  filename = line.replace('   --- CUT: ', '')
  filename = filename.strip().replace(' ---', '')
  svg_file_light = open(filename + '.light.svg', 'w')
  svg_file_light.write(light_svg)
  svg_file_light.close()
  svg_file_dark = open(filename + '.dark.svg', 'w')
  svg_file_dark.write(dark_svg)
  svg_file_dark.close()
  light_svg = ""
  dark_svg = ""
```

# Write a Package

A package is a set of pipelines and contexts that are added to a node as
a single unit.


## Package format


In general a package consist of three kinds of objects:

 - contexts
 - packages
 - snippets

along with some package metadata and a set of inputs


```(yaml)
# The unique package id.
id: example

# Package Metadata (optional)
name: Example Package
author: Tenzir
description: |
  An example package that is intended to 

# A description of the inputs of the package.
inputs:
  filename:
    name: An example filename
    description: This input
    type: string
    default: 127.0.0.1:5555

# The contexts used by this package.
contexts:
  example-context:
    type: lookup-table
    description: A table that contains example data.


# The pipelines contained
pipelines:
  example-pipeline1:
    name: Pipeline No. 1
    description: An example pipeline
    definition: |
      from {{ inputs.filename }}
      | publish example/data

  example-pipeline2:
    name: Pipeline No. 2
    description: An example pipeline in tql2 format
    definition: |
      // experimental tql-2
      subscribe example/data
      | 
    disabled: true

snippets:
  - name: Example Snippet
    description: Show the contents of the example context
    definition: |
        context inspect example/example-context

```

Within a package, all contexts are created before the pipelines, so it is safe to
reference a context defined within the same package.


# Install a package

There are two ways to install packages: By placing the package definitions on the filesystem,
and by using the `package add` operator.

## Installation from File

[...]

## Installation from Operator

[...]

---
sidebar_custom_props:
  operator:
    transformation: true
---

# python

Executes python code against each event of the input.

:::caution Experimental
This operator is considered experimental.

Its intended purpose is fast, interactive experimentation on small datasets.
It was not optimized or intended for fast performance. In particular, it incurs
a heavy pre-processing cost per event in addition to the unavoidable python
interpreter overhead, and will likely run into limitations for larger workloads.
:::

## Synopsis

```
python [--requirements=<string>] <code>
```

## Requirements

A Python 3 (>=3.10) interpreter must be present on the `PATH`. If the operator
instance is located in a tenzir-node, the `PATH` environment variable of the
node is used.

## Description

This operator executes user-provided python code against each event of the input.

The code describes an event-for-event transformation, ie. it is executed once for
each input event and produces exactly output event.

An implicitly defined `self` variable represents the event. Modify it to alter the
output of the operator. Fields of the event can be accessed with the dot notation.
For example, if the input event contains fields `a` and `b` then the python code
can access and modify them using `self.a` and `self.b`. Similarly, new fields are
added by assigning to `self.fieldname` and existing fields can be removed by deleting
them from `self`. When new fields are added, it is required that the new field has
the same type for every row of the event.

By default, the tenzir node executing the pipeline creates a virtual environment
into which the `tenzir` python package is installed. This behavior can be turned off
in the node configuration using the `plugin.python.create-venvs` boolean option.

The `--requirements` flag can be used to pass additional package dependencies in
the pip format. When it is used, the argument is passed on to `pip install` in a
dedicated virtual environment.

## Examples

Insert or modify the field `x` and set it to the string value "hello, world":

```
python 'self.x = "hello, world"'
```

Clear the contents `out` to remove the implicit input values from the output:

```
python '
  self.clear()
  self.x = 23
'
```

Define a new field `x` as the square root of the field `y`, and remove `y` from the output:

```
python '
  import math
  self.x = math.sqrt(self.y)
  del self.y
'
```

Make use of third party packages:

```
python --requirements "requests=^2.30" '
  import requests
  requests.post("http://imaginary.api/receive", data=self)
'
```

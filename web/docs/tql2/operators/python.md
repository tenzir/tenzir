# python

:::info Requirements
A Python 3 (>=3.10) interpreter must be present in the `PATH` environment
variable of the `tenzir` or `tenzir-node` process.
:::

Executes Python code against each event of the input.

```tql
python code:str, [requirements=str]
python file=str, [requirements=str]
```

## Description

The `python` operator executes user-provided Python code against each event of
the input.

By default, the Tenzir node executing the pipeline creates a virtual environment
into which the `tenzir` Python package is installed. This behavior can be turned
off in the node configuration using the `plugin.python.create-venvs` boolean
option.

:::note Performance
The `python` operator implementation applies the provided Python code to each
input row one bw one. We use
[PyArrow](https://arrow.apache.org/docs/python/index.html) to convert the input
values to native Python data types and back to the Tenzir data model after the
transformation.
:::

### `code: str`

The provided Python code describes an event-for-event transformation, i.e., it
is executed once for each input event and produces exactly output event.

An implicitly defined `self` variable represents the event. Modify it to alter
the output of the operator. Fields of the event can be accessed with the dot
notation. For example, if the input event contains fields `a` and `b` then the
Python code can access and modify them using `self.a` and `self.b`. Similarly,
new fields are added by assigning to `self.fieldname` and existing fields can be
removed by deleting them from `self`. When new fields are added, it is required
that the new field has the same type for every row of the event.

### `file: str`

Instead of providing the code inline, the `file` option allows for passing
a path to a file containing the code the operator executes per event.

### `requirements = str (optional)`

The `requirements` flag can be used to pass additional package dependencies in
the pip format. When it is used, the argument is passed on to `pip install` in a
dedicated virtual environment.

The string is passed verbatim to `pip install`. To add multiple dependencies,
separate them with a space: `requirements="foo bar"`.

## Examples

Insert or modify the field `x` and set it to `"hello, world"`:

```tql
python 'self.x = "hello, world"'
```

Clear the contents of `self` to remove the implicit input values from the
output:

```tql
python '
  self.clear()
  self.x = 23
'
```

Define a new field `x` as the square root of the field `y`, and remove `y` from
the output:

```tql
python '
  import math
  self.x = math.sqrt(self.y)
  del self.y
'
```

Make use of third party packages:

```tql
python '
  import requests
  requests.post("http://imaginary.api/receive", data=self)
', requirements="requests=^2.30"
```

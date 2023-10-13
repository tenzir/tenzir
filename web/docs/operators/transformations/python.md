# python

Executes python code against each row of the input.

:::caution Experimental
This operator is considered experimental.

Its intended purpose is fast, interactive experimentation on small datasets.
It incurs a heavy overhead in addition to the unavoidable python interpreter overhead,
and will likely run into limitations for larger workloads.
:::

## Synopsis

```
python <code>
```

## Requirements

A python interpreter must be present on the `PATH` of the node that executes this pipeline.

The `pytenzir` package must be installed.

## Description

Execute arbitrary python code against each row of the input.

The code describes a row-wise transformation, ie. it is executed once for each row of the input
and produces exactly one row of output.

As a side-effect of invoking the python operator, all nested records will appear flattened in the generated output.
The `unflatten` operator can be used to revert this effect where not desired.

If the input is an event with fields `x`, `y`, `z` the code can access local variables `x` `y` and `z`
set to the correct values. All new local variables defined in the code will be present in the output.
Local variables can be prefixed with a `_` to prevent this from happening.

For convenience, the `sys`, `os` and `math` modules are already available and do not need separate import
statements.

Usage of this operator requires that a `python` binary must be in the `PATH` of the `tenzir-node` executable
and the `pytenzir` module must be installed on the target system.

Additionally, the user must ensure that all imported dependencies are installed on the target system.

### <strip-whitespace>

The operator automatically strips leading whitespace from the input code. This option can be used to disable
this behavior.


## Examples

Define a new field `x` that is set to the string value "hello, world":

```
python 'x = "hello, world"'
```

Define a new field `x` as the square root of the field `y`, and remove `y` from the output.

```
python '
  import math
  x = math.sqrt(y)
  del y
'
```

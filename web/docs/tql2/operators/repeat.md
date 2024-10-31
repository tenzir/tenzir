# repeat

Repeats the input a number of times.

```tql
repeat [count:int]
```

## Description

The `repeat` operator relays the input without any modification, and repeats its
inputs a specified number of times. It is primarily used for testing and when
working with generated data.

:::note Potentially High Memory Usage
Take care when using this operator with large inputs.
:::

### `count: int (optional)`

The number of times to repeat the input data.

If not specified, the operator repeats its input indefinitely.

## Examples

### Repeat input indefinitely

Given the following events:

```tql
{number: 1, "text": "one"}
{number: 2, "text": "two"}
```

The `repeat` operator will repeat them indefinitely, in order:

```tql
repeat
```

```tql
{number: 1, "text": "one"}
{number: 2, "text": "two"}
{number: 1, "text": "one"}
{number: 2, "text": "two"}
{number: 1, "text": "one"}
{number: 2, "text": "two"}
// …
```

### Repeat the first event 5 times

```tql
head 1
repeat 5
```

```tql
{number: 1, "text": "one"}
{number: 1, "text": "one"}
{number: 1, "text": "one"}
{number: 1, "text": "one"}
{number: 1, "text": "one"}
```

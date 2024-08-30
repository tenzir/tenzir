---
sidebar_custom_props:
  operator:
    transformation: true
---

# repeat

Repeats the input a number of times.

## Synopsis

```
repeat [<repetitions>]
```

## Description

The `repeat` operator relays the input without any modification, and repeats its
inputs a specified number of times. It is primarily used for testing and when
working with generated data.

The repeat operator keeps its input in memory. Avoid using it to repeat large
data sets.

### `<repetitions>`

The number of times to repeat the input data.

If not specified, the operator repeats its input indefinitely.

## Examples

Given the following events as JSON:

```json
{"number": 1, "text": "one"}
{"number": 2, "text": "two"}
```

The `repeat` operator will repeat them indefinitely, in order:

```
repeat
```

```json
{"number": 1, "text": "one"}
{"number": 2, "text": "two"}
{"number": 1, "text": "one"}
{"number": 2, "text": "two"}
{"number": 1, "text": "one"}
{"number": 2, "text": "two"}
// …
```

To just repeat the first event 5 times, use:

```
head 1 | repeat 5
```

```json
{"number": 1, "text": "one"}
{"number": 1, "text": "one"}
{"number": 1, "text": "one"}
{"number": 1, "text": "one"}
{"number": 1, "text": "one"}
```

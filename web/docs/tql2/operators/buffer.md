# buffer

An in-memory buffer to improve handling of data spikes in upstream operators.

```tql
buffer [capacity:int, policy=str]
```

## Description

The `buffer` operator buffers up to the specified number of events or bytes in
an in-memory buffer.

By default, operators in a pipeline run only when their downstream operators
want to receive input. This mechanism is called back pressure. The `buffer`
operator effectively breaks back pressure by storing up to the specified number
of events in memory, always requesting more input, which allows upstream
operators to run uninterruptedly even in case the downstream operators of the
buffer are unable to keep up. This allows pipelines to handle data spikes more
easily.

### `capacity: int (optional)`

The number of events or bytes that may be kept at most in the buffer.

Note that every operator already buffers up to `254Ki` events before it starts
applying back pressure. Smaller buffers may decrease performance.

### `policy = str (optional)`

Specifies what the operator does when the buffer runs full.

- `"drop"`: Drop events that do not fit into the buffer. This policy is not
  supported for bytes inputs.
- `"block"`: Use back pressure to slow down upstream operators.

When buffering events, this option defaults to `"block"` for pipelines visible on
the overview page on [app.tenzir.com](https://app.tenzir.com), and to `"drop"`
otherwise. When buffering bytes, this option always defaults to `"block"`.

## Examples

### Buffer up to 10 million events or bytes

Buffer and drop events if downstream cannot keep up:

```tql
buffer 10M, policy="drop"
```

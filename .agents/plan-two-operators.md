# Plan: Buffer as Two Operators

## Background

A single `Operator<T, T>` cannot fully decouple upstream from downstream
with bounded buffering. See the design notes at the top of
`libtenzir/builtins/operators/buffer.cpp` for the full analysis. This plan
implements solution (B): split the buffer into two independently-scheduled
operators connected by shared state.

## Architecture

```
upstream → WriteBuffer<T> ──enqueue──→ [shared state] ──dequeue──→ ReadBuffer<T> → downstream
            Operator<T, void>            Mutex<State>              Operator<void, T>
```

- **WriteBuffer** is a sink (`Operator<T, void>`). It accepts upstream data
  via `process()` and enqueues it into the shared buffer.
- **ReadBuffer** is a source (`Operator<void, T>`). It dequeues from the
  shared buffer via `await_task()`/`process_task()` and pushes downstream.
- The two operators are separate pipeline nodes. The executor schedules them
  independently, so `WriteBuffer::process()` can run concurrently with
  `ReadBuffer::process_task()`.

## Shared state

```cpp
template <class T>
struct SharedBufferState {
  std::queue<T> queue;
  uint64_t size = 0;
  uint64_t capacity = 0;
  bool closed = false;
};

// Shared between WriteBuffer and ReadBuffer via Arc.
template <class T>
struct SharedBuffer {
  Mutex<SharedBufferState<T>> state;
  Notify data_available;   // WriteBuffer signals, ReadBuffer waits
  Notify space_available;  // ReadBuffer signals, WriteBuffer waits
};
```

Both operators hold an `Arc<SharedBuffer<T>>`. The `Arc` is created by
the spawner and captured by both operator constructors.

## WriteBuffer (sink)

```
class WriteBuffer<T> final : public Operator<T, void>
```

### process(input, ctx)

Block policy:

```
while size(input) > 0:
    lock state
    free = capacity - state.size
    if free > 0:
        [lhs, rhs] = split(input, free)
        state.queue.push(lhs)
        state.size += size(lhs)
        input = rhs
        unlock
        data_available.notify_one()
    else:
        unlock
    if size(input) > 0:
        co_await space_available.wait()
```

Blocking on `space_available` is safe: ReadBuffer runs independently and
will dequeue, notifying `space_available`.

Drop policy: same as today — enqueue what fits, drop excess with a warning.

### finalize(ctx)

```
lock state
state.closed = true
unlock
data_available.notify_one()
```

Returns `FinalizeBehavior::done`. WriteBuffer has no output to drain.

### snapshot / prepare_snapshot

The buffer contents belong to the shared state. Both operators must
coordinate during checkpointing. The simplest approach: `prepare_snapshot`
on ReadBuffer drains the buffer downstream (same as today). WriteBuffer's
`prepare_snapshot` and `snapshot` are no-ops.

## ReadBuffer (source)

```
class ReadBuffer<T> final : public Operator<void, T>
```

### await_task(dh)

```
loop:
    lock state
    if not state.queue.empty():
        item = state.queue.front(); state.queue.pop()
        state.size -= size(item)
        unlock
        space_available.notify_one()
        return Option{item}
    if state.closed:
        unlock
        return Option<T>{None}
    unlock
    co_await data_available.wait()
```

### process_task(result, push, ctx)

```
opt = result.as<Option<T>>()
if not opt:
    done_ = true
    return
co_await push(*opt)
```

### state()

Returns `done` when the None sentinel has been received, otherwise
`unspecified`.

### finalize(push, ctx)

ReadBuffer is a source — `finalize` is called when the executor wants
to stop the pipeline (e.g., `head` reached its limit). Drain remaining
buffer contents via push and return `done`.

### prepare_snapshot(push, ctx)

Drain the buffer via push (same as today's implementation).

## Describer / spawner changes

### Problem

The current `Describer::spawner` returns a single `SpawnWith<Args, Input>`,
which produces one `AnyOperator`. The `GenericIr::spawn()` method returns
a single `AnyOperator`. We need to return two.

### IR layer: GenericIr::spawn → vector

`ir::Operator::spawn()` currently returns `AnyOperator`. Change it (or add
a parallel path) so that it can return `std::vector<AnyOperator>`. This
aligns with `ir::pipeline::spawn()`, which already returns
`std::vector<AnyOperator>`.

Option 1 — change the return type:

```cpp
virtual auto spawn(element_type_tag input) && -> std::vector<AnyOperator> = 0;
```

This is a wide change — every `ir::Operator` subclass must be updated.

Option 2 — add an `expand` step that wraps single operators:

Keep the existing `spawn() → AnyOperator` signature. Add a new method:

```cpp
virtual auto spawn_all(element_type_tag input) && -> std::vector<AnyOperator> {
    auto result = std::vector<AnyOperator>{};
    result.push_back(std::move(*this).spawn(input));
    return result;
}
```

`GenericIr` overrides `spawn_all` when the description requests multiple
operators. The executor calls `spawn_all` instead of `spawn`. Existing IR
nodes are unaffected (they inherit the default one-element wrapper).

### Spawner: return multiple operators

Add a new type for multi-operator spawn results:

```cpp
// Spawns a pair (or more) of operators from a single Args.
// Returns a vector of AnySpawn, each producing one operator.
// The first element consumes the input; the last produces the output.
// Intermediate connections are implicit (the executor wires void→void).
using MultiSpawn = std::vector<AnySpawn>;
```

Extend `Description` with an optional multi-spawn path:

```cpp
struct Description {
    // ... existing fields ...
    std::optional<MultiSpawner> multi_spawner;
};
```

Where `MultiSpawner` has the same signature as `Spawner` but returns a
vector of spawn functions instead of one.

### Describer API

Add a method to `Describer` parallel to `spawner()`:

```cpp
template <class MultiSpawnerFn>
auto multi_spawner(MultiSpawnerFn fn) -> void;
```

The lambda receives `DescribeCtx&` and the input type, and returns a
vector of spawn functions. For the buffer:

```cpp
d.multi_spawner([...]<class Input>(DescribeCtx& ctx)
    -> failure_or<Option<MultiSpawnWith<Args, Input>>> {
    if constexpr (std::same_as<Input, table_slice>) {
        auto shared = Arc<SharedBuffer<table_slice>>{std::in_place};
        return std::vector{
            // WriteBuffer: Input → void
            SpawnFn<Args, table_slice, void>{
                [shared](BufferArgs args) {
                    return WriteBuffer<table_slice>{args, shared};
                }
            },
            // ReadBuffer: void → Input
            SpawnFn<Args, void, table_slice>{
                [shared](BufferArgs args) {
                    return ReadBuffer<table_slice>{args, shared};
                }
            },
        };
    }
    // ... same for chunk_ptr ...
});
```

### Type inference

`infer_type` for a multi-spawn description chains through the operators:
input → WriteBuffer (void output) → ReadBuffer (T output) → T. In
practice the output type equals the input type, which the multi_spawner
lambda can communicate directly.

### GenericIr integration

In `GenericIr::spawn_all()`, if the description has a `multi_spawner`,
call it to get the vector of spawn functions, fill args, and produce
a `vector<AnyOperator>`. The executor inserts all of them into the
pipeline in order.

## Migration

### Phase 1: infrastructure

1. Add `spawn_all` to `ir::Operator` with the default single-element
   wrapper.
2. Update `ir::pipeline::spawn` to call `spawn_all` on each operator and
   flatten the results.
3. Add `MultiSpawner` / `multi_spawner()` to `Description` / `Describer`.
4. Update `GenericIr::spawn_all` to handle multi-spawn descriptions.
5. Update `GenericIr::infer_type` to chain through multi-spawn types.

### Phase 2: buffer operator

1. Implement `SharedBuffer<T>` (shared state with `Mutex`, `Notify`).
2. Implement `WriteBuffer<T> : Operator<T, void>`.
3. Implement `ReadBuffer<T> : Operator<void, T>`.
4. Update `buffer_plugin::describe()` to use `multi_spawner()`.
5. Remove the single-operator `Buffer<T>` class.
6. Keep the crtp `write_buffer_operator` / `read_buffer_operator` for the
   legacy executor path (they already work correctly).

### Phase 3: cleanup

1. Once the legacy executor is retired, remove the crtp operators and the
   actor-based buffer state.
2. Remove the `operator_parser_plugin` / `operator_factory_plugin`
   inheritance from `buffer_plugin`.

## Verification

The buffer must satisfy all four requirements from the design notes:

- **(R1)** WriteBuffer::process() and ReadBuffer::process_task() run
  independently. Upstream fills the buffer while downstream is slow. ✓
- **(R2)** ReadBuffer::await_task() wakes on data_available and drains
  actively. ✓
- **(R3)** Single shared buffer bounded by capacity. No secondary queues. ✓
- **(R4)** WriteBuffer::finalize() sets closed. ReadBuffer sees closed +
  empty → returns None → done. ✓

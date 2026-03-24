# Porting an Operator from crtp_operator to Operator

This document summarises the steps required to port a TQL operator from the
legacy `crtp_operator` (old executor) to `Operator<Input, Output>` (new
executor). It is based on migrations of many operators including `timeshift`,
`batch`, `buffer`, `delay`, `enumerate`, `assert`, `fork`, `measure`,
`sockets`, `every`, `cron`, and the XSV format operators.

Reference files (pick the one closest to your operator):

| File | Pattern |
|------|---------|
| `libtenzir/builtins/operators/timeshift.cpp` | Simple streaming transformation |
| `libtenzir/builtins/operators/enumerate.cpp` | Streaming with snapshotted mutable state |
| `libtenzir/builtins/operators/batch.cpp` | Buffering with timeout-driven flush |
| `libtenzir/builtins/operators/buffer.cpp` | Producer-consumer queue with mutex/notify |
| `libtenzir/builtins/operators/sockets.cpp` | One-shot source |
| `libtenzir/builtins/operators/every_cron.cpp` | Recurring sub-pipeline spawning |
| `libtenzir/builtins/formats/xsv.cpp` | Format parser/printer (`chunk_ptr`) |

See also: `executor.md` for the full API reference.

---

## Overview

| Aspect          | Old executor (`crtp_operator`)                            | New executor (`Operator<Input, Output>`)      |
|-----------------|-----------------------------------------------------------|-----------------------------------------------|
| Base class      | `crtp_operator<Derived>`                                  | `Operator<Input, Output>`                     |
| Execution model | CAF generators / `co_yield`                               | Folly coroutines / `co_await`                 |
| Data flow       | `operator()(generator<In>, ctrl)`                         | `process()`, `await_task()`, `process_task()` |
| Async I/O       | `ctrl.set_waiting(true)` + CAF request/response           | `co_await async_mail(...).request(actor)`     |
| Serialization   | `friend auto inspect(...)` + `operator_inspection_plugin` | `snapshot(Serde&)` (mutable state only)       |
| Plugin wiring   | `operator_plugin<T>` or `operator_factory_plugin`         | `OperatorPlugin` with `describe()`            |

Keep the old `crtp_operator` in place for backward compatibility (TQL1 /
serialized pipelines). The new executor path is added alongside it. Porting
is also a good opportunity to drop obsolete parameters that accumulated in
the old implementation.

---

## Step 1 — New includes

```cpp
#include <tenzir/async.hpp>           // Operator<>, Task, Push, FinalizeBehavior, Serde, …
#include <tenzir/async/mutex.hpp>     // Mutex<T>  (if needed)
#include <tenzir/async/notify.hpp>    // Notify  (if needed)
#include <tenzir/option.hpp>          // Option<T>
#include <tenzir/operator_plugin.hpp> // OperatorPlugin, Describer, DescribeCtx, Empty
#include <tenzir/try.hpp>             // TRY
```

`operator_plugin.hpp` already includes `async.hpp`, so you only need one of them.

---

## Step 2 — Args struct

Collect all operator parameters into a plain struct.

```cpp
struct MyArgs {
  located<uint64_t> capacity = {};           // located<T> when .source needed in errors
  std::optional<located<std::string>> policy; // optional named arg for Describer
  double speed = 1.0;                        // bare T when location not needed
};
```

- `located<T>` — use when you need `.source` to point errors at the right token.
- `std::optional<located<T>>` — maps to an optional named argument in `Describer`.
- When `Args` contains types that require CAF serialization (`ast::expression`,
  `ast::field_path`), add `friend auto inspect(auto& f, Args& x)` so the old
  executor path can still serialize it.

---

## Step 3 — Operator class

```cpp
class MyOperator final : public Operator<table_slice, table_slice> {
public:
  explicit MyOperator(MyArgs args) : args_{std::move(args)} {}

  auto start(OpCtx& ctx) -> Task<void> override;           // one-time setup
  auto process(table_slice input, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override;                                  // per-input item
  auto await_task(diagnostic_handler& dh) const -> Task<Any> override; // concurrent driver
  auto process_task(Any result, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override;                                  // handles await_task result
  auto prepare_snapshot(Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override;                                  // flush before checkpoint
  auto finalize(Push<table_slice>& push, OpCtx& ctx)
    -> Task<FinalizeBehavior> override;                      // end-of-stream
  auto state() -> OperatorState override;                    // early termination signal
  auto snapshot(Serde& serde) -> void override;              // checkpoint state

private:
  MyArgs args_;
};
```

Override only what you need. Most operators only need `process()`.

---

## Step 4 — Method mapping

### Streaming operators

Replace the generator loop body with `process()`. `co_await push(x)` replaces
`co_yield x`. Empty-slice heartbeat guards (`if rows() == 0 co_yield {}`) are
not needed — the new executor does not deliver zero-row slices to `process()`.

```cpp
auto process(table_slice input, Push<table_slice>& push, OpCtx& ctx)
  -> Task<void> override {
  co_await push(transform(input));
}
```

### Buffering operators (collect-then-emit)

Buffer in `process()`, emit in `finalize()`:

```cpp
auto process(table_slice input, Push<table_slice>& push, OpCtx& ctx)
  -> Task<void> override {
  TENZIR_UNUSED(push, ctx);
  buffer_.push_back(std::move(input));
  co_return;
}

auto finalize(Push<table_slice>& push, OpCtx& ctx)
  -> Task<FinalizeBehavior> override {
  for (auto& s : buffer_) { co_await push(std::move(s)); }
  co_return FinalizeBehavior::done;
}
```

### Timeout-driven flush

When a flush must fire independently of incoming data, drive it through
`await_task()` / `process_task()` as a concurrent timer rather than checking
the clock only inside `process()`. `process()` signals a `Notify` when the
first buffer is created; `await_task()` sleeps on that `Notify` when idle and
otherwise sleeps until the earliest deadline; `process_task()` flushes all
expired buffers.

**Duration overflow**: never compute `start + duration::max()`. Guard sentinel
values before arithmetic:

```cpp
if (timeout_ != duration::max() and now - entry.start_time > timeout_) { … }
```

### Producer-consumer queue

When `process()` (writer) and `await_task()` (reader) run concurrently, use
`Mutex<State>` + `Notify` for coordination. `await_task()` returns `Option<T>`
with `None{}` as the done sentinel. `finalize()` marks the queue closed,
signals the reader, and returns `FinalizeBehavior::continue_` to keep draining.
`state()` returns `done` after `process_task()` observes the sentinel.

### Sources

Sources use `await_task()` to produce data and `process_task()` to push it.
`state()` returns `done` when the source is exhausted. For a one-shot source
(fetches data once and stops), see `sockets.cpp`.

### Sub-pipeline operators

Use `ctx.spawn_sub(key, pipeline, tag_v<Input>)`. The executor calls
`finish_sub()` when a sub-pipeline completes. In `start()`, check for an
existing active sub before spawning to avoid double-spawning on snapshot restore.
After each spawn the active sub key is the one just below the next-to-assign key,
so always use `ctx.get_sub(next_ - 1)` to access it (guard against underflow
when `next_ == 0`).

### Format operators (chunk_ptr ↔ table_slice)

Parsers: `Operator<chunk_ptr, table_slice>`. Use `start()` to initialize parser
state, `process()` to parse chunks, and `finalize()` to flush the last partial
record.

---

## Step 5 — Member types

### Non-movable types (`Mutex<T>`, `Notify`)

Heap-allocate with `Box<T>{std::in_place, …}` and mark `mutable` (required
because `await_task` is `const`):

```cpp
mutable Box<Mutex<State>> state_{std::in_place, State{}};
mutable Box<Notify> data_available_{std::in_place};
```

### Vocabulary preferences

| Prefer | Instead of |
|--------|-----------|
| `Option<T>` | `std::optional<T>` for operator-internal members |
| `Box<T>` | `std::unique_ptr<T>` |
| `enum class Lifecycle { running, draining, done }` | multiple `bool` flags for complex shutdown |

`std::optional` is still required on `Args` struct members wired into
`Describer`.

### Lifecycle modeling for complex shutdown

Use a `Lifecycle` enum rather than multiple booleans when the operator has
distinct shutdown phases. When `stop()` and `finalize()` share the same
teardown sequence, extract a private helper to avoid the two paths diverging:

```cpp
auto teardown(OpCtx& ctx) -> Task<void> {
  if (lifecycle_ == Lifecycle::done) { co_return; }
  lifecycle_ = Lifecycle::done;
  // … shared cleanup …
}

auto finalize(Push<table_slice>& push, OpCtx& ctx)
  -> Task<FinalizeBehavior> override {
  TENZIR_UNUSED(push);
  co_await teardown(ctx);
  co_return FinalizeBehavior::done;
}

auto stop(OpCtx& ctx) -> Task<void> override { co_await teardown(ctx); }
```

---

## Step 6 — snapshot()

Serialize **only mutable state that cannot be derived from `args_`**. Args are
reconstructed from the IR on restart.

```cpp
auto snapshot(Serde& serde) -> void override {
  serde("count", count_);
  serde("last_started", last_started_);
}
```

**Serialize all flags that drive the state machine.** Omitting a flag leaves
it at its zero-value on restore and can cause missed respawns or stalled
pipelines.

**`steady_clock` values are not portable across restarts.** If the operator
uses `steady_clock::time_point` for timing and incorrect offsets after restart
are acceptable, leave `snapshot()` empty and document why. Likewise, if
`prepare_snapshot()` flushes all buffered data first, `snapshot()` is a no-op.

---

## Step 7 — prepare_snapshot()

Drain buffered output before the checkpoint. **Never hold a mutex while
`co_await`-ing `push()`** — that deadlocks because `push()` can block on
downstream backpressure. Take the item under the lock, release it, then push:

```cpp
auto prepare_snapshot(Push<T>& push, OpCtx& ctx) -> Task<void> override {
  TENZIR_UNUSED(ctx);
  while (true) {
    auto item = Option<T>{None{}};
    {
      auto guard = co_await state_->lock();
      if (guard->queue.empty()) { co_return; }
      item = std::move(guard->queue.front());
      guard->queue.pop();
    }                               // lock released before push
    co_await push(std::move(*item));
  }
}
```

---

## Step 8 — Plugin registration

### Adding to an existing plugin

```cpp
class my_plugin final
  : public virtual operator_parser_plugin,   // TQL1
    public virtual operator_factory_plugin,  // TQL2 old path
    public virtual OperatorPlugin {          // TQL2 new path
public:
  auto describe() const -> Description override {
    auto d = Describer<MyArgs, MyOperator>{};
    auto cap = d.positional("capacity", &MyArgs::capacity);
    d.validate([cap](DescribeCtx& ctx) -> Empty {
      TRY(auto c, ctx.get(cap));
      if (c.inner == 0) {
        diagnostic::error("capacity must be greater than zero")
          .primary(c.source).emit(ctx);
      }
      return {};
    });
    return d.without_optimize();
  }
  // … existing parse_operator(), make(), signature() unchanged …
};
```

### New file (no existing old path)

```cpp
class plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override { return "tql2.my_op"; }
  auto describe() const -> Description override { … }
};
TENZIR_REGISTER_PLUGIN(tenzir::plugins::my_op::plugin)
```

### `without_optimize()` vs `order_invariant()`

- `d.without_optimize()` — operator must not be reordered (most operators).
- `d.order_invariant()` — operator is a pure row transform safe to reorder
  (e.g., `assert`, `enumerate`).

### Multiple input types

```cpp
auto d = Describer<MyArgs, MyOp<table_slice>, MyOp<chunk_ptr>>{};
```

### Type-specific validation with `spawner()`

Use `spawner()` when validation or instantiation depends on the input type:

```cpp
d.spawner([]<class Input>(DescribeCtx& ctx)
            -> failure_or<Option<SpawnWith<MyArgs, Input>>> {
  if constexpr (std::same_as<Input, table_slice>) {
    return [](MyArgs args) { return MyOp<table_slice>{std::move(args)}; };
  } else if constexpr (std::same_as<Input, chunk_ptr>) {
    return [](MyArgs args) { return MyOp<chunk_ptr>{std::move(args)}; };
  } else {
    return {}; // void not accepted
  }
});
```

Use `Describer<MyArgs>{}` (no Impl template args) when providing a full spawner.

### Validating a sub-pipeline's output type

Check that the output is both present **and** of the right type. An absent
output (`std::nullopt`) means the sub-pipeline is a sink and is incompatible
with an operator that produces events:

```cpp
TRY(auto output, p.inner.infer_type(tag_v<Input>, ctx));
if (not output or *output != tag_v<table_slice>) {
  diagnostic::error("subpipeline must produce events")
    .primary(p.source).emit(ctx);
  return failure::promise();
}
```

---

## Common pitfalls

### Extract shared logic into free functions

When old and new implementations share logic, extract it into a free function
that both classes call. Do not duplicate.

### Unused boolean flags

Flags declared but never read hide stale state. Either use every flag in the
relevant condition or delete it. This is especially common after refactoring
shutdown logic.

### Don't hold the mutex while `co_await`-ing `push()`

This deadlocks. Always release the lock before awaiting push (see Step 7).

### Don't share argument parsing between old and new paths

The `Describer` validate callback will duplicate some logic from
`parse_operator()` / `make()`. This is intentional — the old operators will
eventually be removed.

### Duration arithmetic overflow

`duration::max()` added to a time_point overflows. Guard sentinel values:

```cpp
if (timeout_ != duration::max() and now - start > timeout_) { … }
```

---

## Testing

- Move existing tests from `test-legacy/` to `test/` when porting.
- Add tests for error paths (bad argument values, incompatible sub-pipelines).

---

## Checklist

- [ ] `Args` struct defined; `located<T>` where source location needed for errors
- [ ] `Args` uses `std::optional` (not `Option`) for optional named args in `Describer`
- [ ] `Args` has `friend auto inspect(...)` if it contains CAF-serializable complex types
- [ ] Operator derives from `Operator<Input, Output>`
- [ ] `process()` does not guard on `rows() == 0`
- [ ] `await_task()` / `process_task()` added only when independent output is needed
- [ ] `prepare_snapshot()` releases lock before `co_await push()`
- [ ] `snapshot()` serializes all state-machine flags (not just counters)
- [ ] `snapshot()` documents why `steady_clock` values are excluded (if applicable)
- [ ] `state()` returns `done` for early-terminating operators
- [ ] Non-movable members wrapped in `Box<T>{std::in_place,…}` and marked `mutable`
- [ ] `Option<T>` used for internal optional members; `std::optional` only for Args
- [ ] `Box<T>` used instead of `std::unique_ptr<T>`
- [ ] Duration overflow guarded when a `duration::max()` sentinel is possible
- [ ] Complex shutdown uses `enum class Lifecycle`; `stop()`/`finalize()` share a teardown helper
- [ ] Sub-pipeline active key accessed via `ctx.get_sub(next_ - 1)`, guarded for `next_ == 0`
- [ ] `OperatorPlugin` added to plugin base classes; `describe()` implemented
- [ ] `validate()` checks argument values; type-specific checks in `spawner()`
- [ ] Sub-pipeline output checked with `not output or *output != tag_v<…>`
- [ ] `d.order_invariant()` used for pure row transforms; `d.without_optimize()` otherwise
- [ ] Tests migrated from `test-legacy/` to `test/`; error-path tests added
- [ ] Old `crtp_operator` and its plugin registrations left intact

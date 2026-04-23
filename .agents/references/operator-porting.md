# Porting an Operator from crtp_operator to Operator

This document summarises the steps required to port a TQL operator from the
legacy `crtp_operator` (old executor) to `Operator<Input, Output>` (new
executor). It is based on migrations of many operators including `timeshift`,
`batch`, `buffer`, `delay`, `enumerate`, `assert`, `fork`, `measure`,
`sockets`, `every`, `cron`, and the XSV format operators.

Reference files (pick the one closest to your operator):

| File                                          | Pattern                                   |
|-----------------------------------------------|-------------------------------------------|
| `libtenzir/builtins/operators/timeshift.cpp`  | Simple streaming transformation           |
| `libtenzir/builtins/operators/enumerate.cpp`  | Streaming with snapshotted mutable state  |
| `libtenzir/builtins/operators/batch.cpp`      | Buffering with timeout-driven flush       |
| `libtenzir/builtins/operators/buffer.cpp`     | Producer-consumer queue with mutex/notify |
| `libtenzir/builtins/operators/sockets.cpp`    | One-shot source                           |
| `libtenzir/builtins/operators/every_cron.cpp` | Recurring sub-pipeline spawning           |
| `libtenzir/builtins/formats/xsv.cpp`          | Format parser/printer (`chunk_ptr`)       |

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
  located<uint64_t> capacity = {};     // located<T> when .source needed in errors
  Option<located<std::string>> policy; // optional named arg for Describer
  double speed = 1.0;                  // bare T when location not needed
};
```

- `located<T>` — use when you need `.source` to point errors at the right token.
- `Option<located<T>>` — maps to an optional named argument in `Describer`.
- Put stable defaults directly in the args struct. Prefer a member initializer
  over a `normalize_*_args()` helper when the default does not depend on
  runtime context.

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
  auto finalize(Push<table_slice>& push, OpCtx& ctx)
    -> Task<FinalizeBehavior> override;                      // end-of-stream
  auto state() -> OperatorState override;                    // early termination signal
  auto prepare_snapshot(Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override;                                  // flush before checkpoint
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

When the operator batches rows in a `series_builder`, use `SeriesPusher` from
`tenzir/async/pusher.hpp` together with `series_builder::yield_ready()`. Call
`yield_ready()` from `process()` after appending rows and again from
`process_task()` for timeout-driven flushes. Keep explicit flushes for
semantic boundaries such as header transitions, document-close markers,
snapshots, and finalization. The same applies to operators that use
`multi_series_builder`.

Do not perform timeout checks while processing some input. Only do one timeout
check after processing input.

**Duration overflow**: never compute `start + duration::max()`. Guard sentinel
values before arithmetic:

```cpp
if (timeout_ != duration::max() and now - entry.start_time > timeout_) { … }
```

### Sources

Sources use `await_task()` to produce data and `process_task()` to push it.
`state()` returns `done` when the source is exhausted. For a one-shot source
(fetches data once and stops), see `sockets.cpp`.

### Format operators (chunk_ptr -> table_slice)

`read_xxx` operators typically implement `Operator<chunk_ptr, table_slice>`.
They use `start()` to initialize a parser state, `process()` to parse chunks,
and `finalize()` to flush the last partial record.

When `process()` scans a chunk incrementally, accumulate a local
`series_builder::YieldReadyResult`, merge `yield_ready_as_table_slice(...)`
into it as records become ready, and push once after the loop. Reference
implementations: `read_cef`, `read_leef`, `read_xsv`, and `read_kv`.

### Blocking operations

File I/O, subprocess calls, and third-party synchronous SDKs must not run
directly on an executor thread — they would stall every coroutine sharing
that thread. Wrap such calls in `co_await spawn_blocking(...)` from
`tenzir/async/blocking_executor.hpp`, which offloads the callable to a
dedicated CPU thread pool (Tokio-aligned defaults: up to 512 threads, 10 s
idle timeout) and resumes the coroutine with its return value:

```cpp
auto bytes = co_await spawn_blocking([path = path_] {
  return blocking_file_read(path);
});
```

`spawn_blocking` expects a synchronous callable. Passing a coroutine
function returning `Task<T>` only constructs the coroutine handle on the
pool thread without running its body — use folly's `co_withExecutor` /
`scheduleOn` for that instead. Exceptions thrown by the callable are
captured and rethrown at the await site. Reference implementations:
`from_file.cpp`, `file.cpp`, `python.cpp`, and the cloud `from_*` operators
under `plugins/s3`, `plugins/gcs`, and `plugins/azure-blob-storage`.

### Diagnostics

Some diagnostics in the old executor do not add a location via `primary`. Some
operators also make use of a transforming diagnostic handler that modifies diagnostics.

If a diagnostic does not contain a location, use the operators location. Capture
this location in the `Describer` via `describer.operator_location(&MyArgs::operator_location)`.

If a transforming diagnostic handler is used to modify the diagnostic to hint at
the operator, such as prepending a hint at the operator to the diagnostic, instead
add the operators location to these diagnostics.

---

## Step 5 — Member types

### Vocabulary preferences

| Prefer                                             | Instead of                                       |
|----------------------------------------------------|--------------------------------------------------|
| `Option<T>`                                        | `std::optional<T>` for operator-internal members |
| `Box<T>`                                           | `std::unique_ptr<T>`                             |
| `enum class Lifecycle { running, draining, done }` | multiple `bool` flags for complex shutdown       |
| `Atomic<T>`                                        | `std::atomic<T>`                                 |
| `Ref<T>`                                           | `std::reference_wrapper<T>`                      |
| `Mutex<T>`                                         | `std::mutex` + separate data                     |

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

**Buffered data should be flushed in prepare_snapshot.** If the operator contains
dynamically large amount of data (i.e. a buffer for incoming data), it should
not be serialized in `snapshot()`. Instead, there is `prepare_snapshot()` for
emitting this data before yielding and allowing `snapshot()` to run.

---

## Step 7 — Plugin registration

### `DescribeCtx::get()` and defaulted named arguments

Inside `d.validate(...)`, `DescribeCtx::get()` returns `std::nullopt` when the
caller omitted an argument. This also applies to `named_optional(...)`
arguments whose target member in `Args` has a default initializer.

Do not assume that `ctx.get(arg)` is engaged just because `Args` carries a
member default. Apply the args-struct defaults explicitly:

```cpp
auto defaults = MyArgs{};
auto timeout = ctx.get(timeout_arg);
auto effective_timeout = timeout ? *timeout : defaults.timeout;
```

Use `.value()` only when the argument is truly required.

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

### New file

If the old implementation is already a huge file or highly complex,
it is preferred to create a new file for the new operator.
When doing so, a new Plugin class will have to be defined.
It's name should not clash with the name of the old impl. This can
be avoided by adding or removing `tql2.` prefix. It does not matter
which form is used by the new impl.

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

Only use a `spawner` if the intended behaviour cannot be modeled via a `Describer` alone.

### Validating a sub-pipeline's output type

Check that the output is both present **and** of the right type. An absent
output (`std::nullopt`) means the sub-pipeline is not known yet (which is
something that should probably be eventually removed).
If the sub-pipeline is a sink the inferred type is `tag_v<void>`.

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

### Don't extract shared logic

Given that the old implementation will eventually be removed, don't try to
extract shared logic from the old implementation. If old implementation uses
existing helpers, they can be reused if they don't require major modifications.

### Unused boolean flags

Flags declared but never read hide stale state. Either use every flag in the
relevant condition or delete it. This is especially common after refactoring
shutdown logic.

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
- [ ] Operator derives from `Operator<Input, Output>`
- [ ] `process()` does not guard on `rows() == 0`
- [ ] `await_task()` / `process_task()` added only when independent output is needed
- [ ] `prepare_snapshot()` releases lock before `co_await push()`
- [ ] `snapshot()` serializes all state-machine flags (not just counters)
- [ ] `snapshot()` documents why `steady_clock` values are excluded (if applicable)
- [ ] `state()` returns `done` for early-terminating operators
- [ ] `Box<T>` used instead of `std::unique_ptr<T>`
- [ ] Duration overflow guarded when a `duration::max()` sentinel is possible
- [ ] Blocking calls (file I/O, subprocess, sync SDKs) wrapped in `co_await spawn_blocking(...)`
- [ ] Complex shutdown uses `enum class Lifecycle`; `stop()`/`finalize()` share a teardown helper
- [ ] Sub-pipeline active key accessed via `ctx.get_sub(next_ - 1)`, guarded for `next_ == 0`
- [ ] `OperatorPlugin` added to plugin base classes; `describe()` implemented
- [ ] `validate()` checks argument values; type-specific checks in `spawner()`
- [ ] Sub-pipeline output checked with `not output or *output != tag_v<…>`
- [ ] `d.order_invariant()` used for pure row transforms; `d.without_optimize()` otherwise
- [ ] Tests migrated from `test-legacy/` to `test/`; error-path tests added
- [ ] Old `crtp_operator` and its plugin registrations left intact

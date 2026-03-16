# Executor

Implementing operators for pipeline execution.

## API Reference

The executor API is defined in `<tenzir/async.hpp>`. Key classes:

- `Operator<Input, Output>` - Base class for all operators
- `OperatorBase` - Provides `state()`, `snapshot()`, `await_task()`
- `OperatorOutputBase<Output>` - Provides `process_task()`, `finalize()`
- `OperatorInputOutputBase<Input, Output>` - Provides `process()`

See the docstrings in `async.hpp` for detailed invariants and usage patterns.

## Type Combinations

| Input         | Output        | Use Case                |
| ------------- | ------------- | ----------------------- |
| `void`        | `table_slice` | Source (event producer) |
| `table_slice` | `table_slice` | Transformation          |
| `table_slice` | `void`        | Sink (event consumer)   |
| `chunk_ptr`   | `table_slice` | Parser                  |
| `table_slice` | `chunk_ptr`   | Serializer              |

## Guidelines

### Snapshot

When overriding `snapshot(Serde&)`, only process **mutable** state that cannot
be derived from arguments. Immutable state is restored from operator arguments
on restart.

### State

Only override `state()` if the operator can terminate early (e.g., `head` stops
after N rows). Operators that process until end-of-stream do not need to
override `state()`.

### Avoid Destructors

Avoid defining destructors for operators. Work that needs to be done during a
regular shutdown should be performed in `finalize()`. Work that needs to always
be, including in the case of cancellation should be handled via RAII members. If
you need to define a destructor because no RAII type is easily available, make
sure to `= delete` the copy operations and `= default` the move operations of the
operator class.

### Variant Dispatch in Coroutines

Use `co_match` instead of `match` — see `variant-access.md`.

### Message-queue coordination

Helper tasks must not mutate operator members. Instead, define a typed message
variant and a bounded queue. The helper writes messages; `process_task()` is
the single place that updates operator state.

Reference implementations: `accept_tcp.cpp`, `from_tcp.cpp`, `serve_tcp.cpp`,
`to_tcp.cpp` under `libtenzir/builtins/operators/`.

```cpp
// Define one message per event kind the operator cares about.
struct Accepted { Box<folly::coro::Transport> client; };
struct ConnectionClosed { uint64_t conn_id; Option<std::string> error; };
using Message = variant<Accepted, ConnectionClosed>;
using MessageQueue = folly::coro::BoundedQueue<Message>;
```

- `await_task()` dequeues the next message.
- `process_task()` dispatches it with `co_match` and updates members.
- The queue provides backpressure for free.

### Lifecycle modeling

Use an enum, not booleans. A `done_` flag hides an incomplete state machine as
soon as a second task or shutdown path appears.

```cpp
enum class Lifecycle { running, draining, done };
```

Transition in `process_task()` and `finalize()`. Check in `state()`.

### Cancellation

Wrap long-running loops with `folly::CancellationSource`:

```cpp
co_await folly::coro::co_withCancellation(cancel_.getToken(), accept_loop());
```

Inside helpers, obtain the token with
`co_await folly::coro::co_current_cancellation_token` and check it at loop
boundaries. Shutdown sequence:

1. Call `cancel_.requestCancellation()`.
2. Close or cancel the I/O object on its owning executor.
3. Wait for the helper to finish (notification or RAII guard).

Do not add a second `stop_` flag—cancellation tokens already express this.

## Patterns

### Streaming (head, filter)

Operators that emit output immediately use `process()`:

```cpp
auto process(table_slice input, Push<table_slice>& push, OpCtx& ctx)
  -> Task<void> override {
  co_await push(transform(input));
}
```

### Buffering (tail, sort)

Operators that need all input before producing output buffer in `process()`
and emit in `finalize()`:

```cpp
auto process(table_slice input, Push<table_slice>& push, OpCtx& ctx)
  -> Task<void> override {
  TENZIR_UNUSED(push, ctx);
  buffer_.push_back(std::move(input));
  co_return;
}

auto finalize(Push<table_slice>& push, OpCtx& ctx) -> Task<void> override {
  for (auto& slice : buffer_) {
    co_await push(std::move(slice));
  }
}
```

### Sources (from, version)

Source operators use `await_task()` + `process_task()` instead of `process()`.
The `await_task()` method awaits an external task (network I/O, timer, etc.)
and returns its result via `Any`. This is heavily implementation-specific:

```cpp
auto await_task() const -> Task<Any> override {
  // Await external work: network read, timer, file I/O, etc.
  auto data = co_await fetch_from_network();
  co_return data;
}

auto process_task(Any result, Push<table_slice>& push, OpCtx& ctx)
  -> Task<void> override {
  auto data = std::move(result).as<MyData>();
  co_await push(to_table_slice(data));
  count_ += 1;
}

auto state() -> OperatorState override {
  return count_ >= total_ ? OperatorState::done : OperatorState::unspecified;
}
```

## Cross-Executor I/O (folly EventBase)

Socket operations via folly's `AsyncSocket` / `Transport` must run on the
owning `EventBase` thread. Use `co_withExecutor` to schedule each call there:

```cpp
auto n = co_await co_withExecutor(evb_, transport_->read(buf, timeout));
```

Do not use `co_viaIfAsync` — it only controls where the caller resumes, not
where the task executes. Do not use the deprecated `.scheduleOn()` method.

For multi-statement blocks, wrap with `co_invoke`:

```cpp
co_await co_withExecutor(evb_,
  folly::coro::co_invoke([&]() -> folly::coro::Task<folly::Unit> {
    ssl_ptr->sslConn(&cb);
    co_await cb.wait();
    co_return folly::unit;
  }));
```

## Plugin Declaration

Use `Describer` to register operators:

```cpp
class plugin final : public virtual operator_parser_plugin,
                     public virtual operator_compiler_plugin {
public:
  auto describe() const -> Description override {
    auto d = Describer<Args, Impl>{};
    d.optional_positional("count", &Args::count);
    return d.without_optimize();
  }
};
```

## Args Pattern

Store the entire args struct as a member rather than individual fields:

```cpp
struct MyArgs {
  uint64_t count = 10;
};

class MyOperator final : public Operator<table_slice, table_slice> {
public:
  explicit MyOperator(MyArgs args) : args_{args} {}

private:
  MyArgs args_;  // Store entire struct, not individual members
};
```

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

Helper tasks must not mutate operator members. This in particular applies to
tasks spawned with `ctx.spawn_task()`, as well as `await_task()`, as those tasks
run concurrently with the other functions. When more than a single task needs to
be awaited on, use a bounded queue to read from in `await_task()` which is
written to by the other tasks. The messages then arrive in `process_task()`,
which is then allowed to update operator state. Similarly, communication from
`process_task()` and the other operator functions such as `process()` to
asynchronous tasks should be done via channels.

References: `accept_tcp.cpp`, `from_tcp.cpp`, `serve_tcp.cpp`, `to_tcp.cpp`.

```cpp
// Define one message per event kind the operator cares about.
struct Accepted { folly::coro::Transport client; };
struct ConnectionClosed { uint64_t conn_id; Option<std::string> error; };
using Message = variant<Accepted, ConnectionClosed>;
using MessageQueue = folly::coro::BoundedQueue<Message>;
```

- `await_task()` dequeues the next message.
- `process_task()` dispatches it with `co_match` and updates members.
- The queue provides backpressure for free.

### Cancellation

Do not customize cancellation unless necessary. The framework will by default
cancel all tasks when the pipeline is stopped. You must customize cancellation
if it's needed for the operator to perform correctly. For example, if a HTTP
operator wants to stop accepting connections, it could make sense to stop the
loop that calls `accept` with a custom `folly::CancellationSource`.

```cpp
ctx.spawn_task(folly::coro::co_withCancellation(cancel_.getToken(), accept_loop()));
```

Do not add a second `stop_` flag—cancellation tokens already express this.

Usually, you do not need to check for cancellation explicitly, as many async
operations implicitly do so. You may `co_await folly::coro::co_safe_point` at
the top of loops if needed.

Do not attempt to catch cancellation, neither as an exception nor with
`co_awaitTry`, unless truly necessary. Usually, cancellation should propagate
necessary to the outermost task.

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

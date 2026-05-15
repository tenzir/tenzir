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

Serialize all flags that drive the operator state machine. Omitting a flag can
restore it to a zero value and stall the pipeline.

Do not serialize dynamically large buffers. Flush them in `prepare_snapshot()`
before `snapshot()` runs. If `prepare_snapshot()` flushes all buffered state,
`snapshot()` can be a no-op.

`steady_clock::time_point` values are not portable across restarts. If incorrect
offsets after restart are acceptable, leave them out of `snapshot()` and document
why.

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
- Use `tenzir::Mutex<T>` only for small shared helper state that must be
  touched from multiple tasks.

Use `SeriesPusher` from `tenzir/async/pusher.hpp` when an operator uses
`series_builder::yield_ready()` together with `await_task()` /
`process_task()` to drive timeout-based flushes.

For timeout-driven flushes, let `await_task()` sleep until the earliest deadline
and let `process_task()` flush expired buffers. Do not poll time while processing
each input item. Guard `duration::max()` sentinels before time arithmetic.

### Metrics counters

Operators that form an external pipeline edge should report both byte and event
throughput when both quantities are meaningful. Create counters in `start()` with
`ctx.make_counter(label, direction, visibility, type)` and keep them as operator
members.

- Use `MetricsDirection::read` for ingress/source-side data and
  `MetricsDirection::write` for egress/sink-side data.
- Use `MetricsVisibility::external_` for data crossing the pipeline boundary
  (network, files, stdin/stdout, databases, message brokers). Use internal
  visibility only for executor-internal handoff accounting.
- Use `MetricsUnit::bytes` for encoded bytes read or written and
  `MetricsUnit::events` for event rows.
- If an operator emits `table_slice`s directly and it does not accept a parsing
  (`chunk_ptr -> table_slice`) pipeline, increment the events counter by
  `slice.rows()` immediately before pushing the slice downstream.
- If an operator consumes `table_slice`s directly and it does not accept a
  printing (`table_slice -> chunk_ptr`) pipeline, increment the events counter
  by `slice.rows()` when accepting the slice for output.
- Operators that run parser or printer subpipelines should not count the rows
  flowing through those subpipelines. The nested parser or printer operators own
  those event counters.
- Message-oriented connectors that produce or consume one event per message may
  increment the events counter by `1`; slice-oriented code should use
  `slice.rows()`.

Do not register only a byte counter for an operator that produces or consumes
rows directly. That causes node-level event throughput to report zero for that
operator even though data is flowing.

### Per-chunk parser flushes

If a `chunk_ptr -> table_slice` parser scans an input chunk incrementally,
accumulate a `series_builder::YieldReadyResult` for the whole chunk, merge
`yield_ready_as_table_slice(...)` into it as records become ready, and push
once after the loop.

This keeps parsing state changes local to `process()` and matches the existing
parser patterns in `read_cef`, `read_leef`, `read_xsv`, and `read_kv`.

### Blocking calls

File I/O, subprocess calls, and synchronous SDK calls must not run directly on
an executor thread. Wrap them in `co_await spawn_blocking(...)` from
`tenzir/async/blocking_executor.hpp`.

```cpp
auto bytes = co_await spawn_blocking([path = path_] {
  return blocking_file_read(path);
});
```

`spawn_blocking` expects a synchronous callable. Passing a coroutine that returns
`Task<T>` only constructs the coroutine handle on the blocking pool.

### Structured concurrency with `async_scope`

Use `async_scope()` for fan-out within a task. If a task spawns child tasks and
must wait for them before returning, wrap that code in `async_scope()`.

```cpp
co_await async_scope([&](AsyncScope& scope) -> Task<void> {
  while (has_more_work()) {
    scope.spawn(handle_one());
  }
});
```

References: `accept_tcp.cpp`, `serve_tcp.cpp`.

### Semaphores for capacity limits

Use `tenzir::Semaphore` for resource permits. Do not use atomics or
ad-hoc counters for permit accounting. Keep using message queues for state
coordination.

Acquire the permit before starting the operation that creates or reserves the
resource. Release permits via RAII in the task that currently owns them. If
ownership is handed back to the operator via a message, release it from the
matching operator lifecycle path.

```cpp
auto permits_ = Semaphore{limit};

auto guard = co_await permits_.acquire();
co_await start_work();
// Prefer RAII to drop the guard, or alternatively:
guard.release();
```

### Cancellation

Do not customize cancellation unless necessary. The framework will by default
cancel all tasks when the pipeline is stopped. Add custom cancellation only
when the operator needs a separate shutdown path to behave correctly. For
example, a HTTP operator that must stop accepting connections should stop the
`accept` loop with a custom `folly::CancellationSource`.

When you pass a custom token to `co_withCancellation`, merge it with the
ambient token from `co_current_cancellation_token`. Otherwise, the custom token
replaces outer pipeline or executor cancellation for that await.

```cpp
ctx.spawn_task([this]() -> Task<void> {
  auto token = folly::cancellation_token_merge(
    co_await folly::coro::co_current_cancellation_token,
    cancel_.getToken());
  co_await folly::coro::co_withCancellation(token, accept_loop());
});
```

Pass a bare custom token only when you intentionally want to shield the await
from outer cancellation. If local shutdown and outer cancellation need
different behavior, keep both tokens available so you can tell which one fired
and propagate only the outer cancellation.

Do not add a second `stop_` flag—cancellation tokens already express this.

Usually, you do not need to check for cancellation explicitly, as many async
operations implicitly do so.

Use `Notify` for one-shot wakeups. Do not use `folly::coro::Baton` directly in
operators, because it is not cancellation-aware.

Do not attempt to catch cancellation, neither as an exception nor with
`co_awaitTry`, unless truly necessary. Let cancellation propagate to the
outermost task.

For operators with multiple shutdown phases, prefer a `Lifecycle` enum over
several booleans. If `stop()` and `finalize()` share teardown work, put it in one
helper.

### Retry loops

Use `folly::coro::retryWithExponentialBackoff` for retriable async operations
such as reconnects. Do not hand-roll sleep / backoff loops.

```cpp
co_await folly::coro::retryWithExponentialBackoff(
  std::numeric_limits<uint32_t>::max(),
  std::chrono::milliseconds{100},
  std::chrono::seconds{5},
  0.0,
  []() -> Task<void> {
    co_await try_once();
  });
```

References: `from_tcp.cpp`, `to_tcp.cpp`.

## Patterns

### Streaming (head, filter)

Operators that emit output immediately use `process()`:

```cpp
auto process(table_slice input, Push<table_slice>& push, OpCtx& ctx)
  -> Task<void> override {
  co_await push(transform(input));
}
```

The executor does not deliver zero-row slices to `process()`.

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

auto finalize(Push<table_slice>& push, OpCtx& ctx)
  -> Task<FinalizeBehavior> override {
  for (auto& slice : buffer_) {
    co_await push(std::move(slice));
  }
  co_return FinalizeBehavior::done;
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
owning `EventBase` thread. Prefer `ctx.io_executor()` inside operators over
reaching for `folly::getGlobalIOExecutor()` directly.

### KeepAlive pattern

`ctx.io_executor()` returns a
`folly::Executor::KeepAlive<folly::IOExecutor>`. If an operator keeps using the
executor after `start()`, store that `KeepAlive` handle as a member. Use
`io_executor_->getEventBase()` only for APIs that require a raw `EventBase*`.
Do not keep only a raw `EventBase*`.

```cpp
io_executor_ = ctx.io_executor();
writer_ = folly::AsyncPipeWriter::newWriter(
  io_executor_->getEventBase(), folly::NetworkSocket::fromFd(fd));
co_await folly::coro::co_withExecutor(io_executor_, do_io());
```

Use `co_withExecutor` to schedule each call there:

```cpp
auto n = co_await co_withExecutor(io_executor_, transport_->read(buf, timeout));
```

Do not use `co_viaIfAsync` — it only controls where the caller resumes, not
where the task executes. Do not use the deprecated `.scheduleOn()` method.

For multi-statement blocks, wrap with `co_invoke`:

```cpp
co_await co_withExecutor(io_executor_,
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

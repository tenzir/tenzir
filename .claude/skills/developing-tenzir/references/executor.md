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
and returns its result via `std::any`. This is heavily implementation-specific:

```cpp
auto await_task() const -> Task<std::any> override {
  // Await external work: network read, timer, file I/O, etc.
  auto data = co_await fetch_from_network();
  co_return data;
}

auto process_task(std::any result, Push<table_slice>& push, OpCtx& ctx)
  -> Task<void> override {
  auto data = std::any_cast<MyData>(std::move(result));
  co_await push(to_table_slice(data));
  count_ += 1;
}

auto state() -> OperatorState override {
  return count_ >= total_ ? OperatorState::done : OperatorState::unspecified;
}
```

## Plugin Declaration

Use `Describer` to register operators:

```cpp
class plugin final : public virtual operator_parser_plugin,
                     public virtual operator_compiler_plugin,
                     public virtual OperatorPlugin {
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

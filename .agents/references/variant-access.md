# Variant Access

Working with variants in Tenzir.

## Use Tenzir Helpers, Not std::

| Use            | Instead of                     |
| -------------- | ------------------------------ |
| `is<T>(x)`     | `std::holds_alternative<T>(x)` |
| `as<T>(x)`     | `std::get<T>(x)`               |
| `try_as<T>(x)` | `std::get_if<T>(&x)`           |

```cpp
if (auto* str = try_as<std::string>(value)) {
  // Use *str
}
```

When you need the value, prefer `try_as<T>` over `is<T>` followed by `as<T>`.
If you would probe the same value for multiple alternatives, use `match`
instead.

## Match for Multi-Case Dispatch

Use `match()` instead of switch statements or if-else chains:

```cpp
auto result = match(
  series,
  [](basic_series<double_type> s) { /* handle double */ },
  [](basic_series<int64_type> s) { /* handle int64 */ },
  [](const auto&) { /* fallback */ },
);
```

Pass visitors directly to `match`; do not wrap them in `detail::overload`.

## `co_match` for Async Dispatch

When variant dispatch happens inside a coroutine and handlers return `Task<>`,
use `co_match` instead of `match`:

```cpp
#include <tenzir/co_match.hpp>

co_await co_match(
  msg,
  [&](ScanComplete& scan) -> Task<void> {
    // async handler…
    co_return;
  },
  [&](ReadComplete& read) -> Task<void> {
    // async handler…
    co_return;
  },
);
```

Use `co_match` when handlers return `Task<>` (async) and `match` for pure value
transforms (sync). Never `co_await match(...)`.

## Applicable Types

Types with `variant_traits` that support `is`, `as`, `try_as`, and `match`:

- `data` — TQL data values
- `series` — Columnar data arrays
- `type` — TQL type system
- `expression` — TQL expressions
- `ast::expression` — AST expression nodes
- `bitmap` — Bitmap variants
- `secret` / `secret_view` — Secret values
- `arrow::DataType` — Arrow type dispatching
- `arrow::Array` — Arrow array dispatching
- `arrow::ArrayBuilder` — Arrow builder dispatching
- `tenzir::variant<Ts...>` — Custom variants
- `std::variant<Ts...>` — Standard library variants

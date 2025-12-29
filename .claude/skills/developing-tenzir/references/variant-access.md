# Variant Access

Working with variants in Tenzir.

## Use Tenzir Helpers, Not std::

| Use            | Instead of                     |
| -------------- | ------------------------------ |
| `is<T>(x)`     | `std::holds_alternative<T>(x)` |
| `as<T>(x)`     | `std::get<T>(x)`               |
| `try_as<T>(x)` | `std::get_if<T>(&x)`           |

```cpp
if (is<int64_t>(value)) {
  auto num = as<int64_t>(value);
}

if (auto* str = try_as<std::string>(value)) {
  // Use *str
}
```

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

## Overload Helper

Combine lambdas with `detail::overload{}`:

```cpp
auto handler = detail::overload{
  [&](const arrow::StringArray& arr) { /* strings */ },
  [&](const arrow::Int64Array& arr) { /* integers */ },
  [&](const auto&) { /* fallback */ },
};
match(std::tie(*array), handler);
```

## Applicable Types

These patterns work on: `data`, `series`, `tenzir::variant`, `std::variant`.

# Utilities

Generic utilities in `tenzir::detail`. Use these instead of hand-rolling.

## Strings — `detail/string.hpp`

```cpp
auto trimmed = detail::trim("  hello  ");              // "hello"
auto front = detail::trim_front("  hello");            // "hello"
auto parts = detail::split("a,b,c", ",");              // {"a", "b", "c"}
auto [first, rest] = detail::split_once("a:b:c", ":"); // {"a", "b:c"}
auto joined = detail::join(vec, ", ");
auto replaced = detail::replace_all(str, "old", "new");
```

## Encoding

- `detail/base64.hpp` — `encode()`, `decode()`, `try_decode()`
- `detail/hex_encode.hpp` — `hex_encode()`, `hex_decode()`
- `detail/escapers.hpp` — JSON, percent, control char escapers

## Containers

- `detail/flat_map.hpp`, `detail/flat_set.hpp` — Sorted vector, O(log n) lookup
- `detail/stable_map.hpp`, `detail/stable_set.hpp` — Insertion-order preserving
- `detail/lru_cache.hpp` — LRU cache with factory function

```cpp
detail::flat_map<std::string, int> sorted_map;

detail::lru_cache<Key, Value> cache{capacity, [](const Key& k) {
  return compute_value(k);
}};
auto& value = cache.get(key);  // Creates if missing
```

## Iteration — `detail/enumerate.hpp`, `<ranges>`

```cpp
for (auto [index, value] : detail::enumerate(container)) { ... }

for (auto [a, b] : std::views::zip(vec1, vec2)) { ... }  // Use C++23 std::views::zip
```

## Numerics

- `detail/narrow.hpp` — `narrow<T>()` (checked, panics on overflow),
  `narrow_cast<T>()` (unchecked)
- `detail/checked_math.hpp` — `checked_add()`, `checked_sub()`, `checked_mul()`
  (returns `std::nullopt` on overflow)
- `detail/saturating_arithmetic.hpp` — `saturating_add()`, `saturating_sub()`,
  `saturating_mul()`
- `detail/byteswap.hpp` — `to_network_order()`, `to_host_order()`

## Comparisons

Use C++20's spaceship operator for comparison. Implement `operator<=>` and
`operator==` to get all comparison operators automatically:

```cpp
struct my_type {
  auto operator<=>(const my_type&) const = default;
  bool operator==(const my_type&) const = default;
  // Automatically provides: <, >, <=, >=, !=
};
```

## Scope Guard — `detail/scope_guard.hpp`

```cpp
auto guard = detail::scope_guard{[]() noexcept { cleanup(); }};
guard.disable();  // Cancel if not needed
guard.trigger();  // Trigger early
```

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

## Iteration — `detail/enumerate.hpp`, `detail/zip_iterator.hpp`

```cpp
for (auto [index, value] : detail::enumerate(container)) { ... }

for (auto [a, b] : detail::zip(vec1, vec2)) { ... }
for (auto [a, b] : detail::zip_equal(vec1, vec2)) { ... }  // Asserts same size
```

## Numerics

- `detail/narrow.hpp` — `narrow<T>()` (checked, panics on overflow),
  `narrow_cast<T>()` (unchecked)
- `detail/saturating_arithmetic.hpp` — `saturating_add()`, `saturating_sub()`,
  `saturating_mul()`
- `detail/byteswap.hpp` — `to_network_order()`, `to_host_order()`

## Operators — `detail/operators.hpp`

CRTP mixins: implement `<` and `==`, get all comparison operators.

```cpp
struct my_type : detail::totally_ordered<my_type> {
  bool operator<(const my_type& other) const;
  bool operator==(const my_type& other) const;
  // Automatically provides: !=, >, <=, >=
};
```

Other mixins: `addable`, `subtractable`, `multipliable`, `dividable`.

## Scope Guard — `detail/scope_guard.hpp`

```cpp
auto guard = detail::scope_guard{[]() noexcept { cleanup(); }};
guard.disable();  // Cancel if not needed
guard.trigger();  // Trigger early
```

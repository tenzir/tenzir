# Data Access

Reading and iterating columnar data in Tenzir.

## Use view3, Not Legacy Views

Use `values3()` for iterating series and `view3<T>` for accessing elements.
These provide zero-copy Arrow-native access without materialization.

```cpp
#include <tenzir/view3.hpp>

// Iterate a series (Arrow array)
for (auto row : values3(array)) {
  if (not row) {
    // Handle null
    continue;
  }
  // Use *row (returns view3<T>)
}
```

## Avoid These Legacy Patterns

- `values()` — Older view system, materializes early
- `value_at()` — From `arrow_table_slice.hpp`, also legacy
- `.values()` method — Use free function `values3()` instead

## Working with Records and Lists

Access nested data through `view3<record>` and `view3<list>`:

```cpp
for (auto&& doc : docs.values3()) {
  if (const auto* rec = try_as<view3<record>>(doc)) {
    for (const auto& [key, value] : *rec) {
      // key is std::string_view, value is data_view3
    }
  }
}
```

## Never Materialize

Do not convert Arrow data to `record` or `list` types. Keep data in columnar
form and operate on it directly during iteration.

## Headers

- `tenzir/view3.hpp` — Core view3 abstractions
- `tenzir/series.hpp` — Series type definition

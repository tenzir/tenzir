# Data Building

Constructing series and table slices in Tenzir.

## Series Builder

Use `series_builder` for row-wise construction with automatic type inference:

```cpp
#include <tenzir/series_builder.hpp>

auto builder = series_builder{};
auto rec = builder.record();
rec.field("name").data("example");
rec.field("count").data(int64_t{42});

auto slices = builder.finish_as_table_slice("my.schema");
```

## Multi-Series Builder

Use `multi_series_builder` for heterogeneous data with multiple schemas:

```cpp
#include <tenzir/multi_series_builder.hpp>

auto msb = multi_series_builder{policy, settings, dh};
auto event = msb.record();
event.field("src").data(src_ip);
event.field("dst").data(dst_ip);
auto result = msb.finalize();
```

## Nested Fields with Field Paths

Use `.field(ast::field_path)` for deeply nested structures:

```cpp
auto path = ast::field_path::try_from(...);
auto nested = builder.record().field(*path).record();
nested.field("value").data(int64_t{1});
```

## Arrow Builder Patterns

When building Arrow arrays directly, always use `check()`:

```cpp
auto b = string_type::make_arrow_builder(arrow_memory_pool());
check(b->Append("value"));
return series{string_type{}, finish(*b)};
```

## Headers

- `tenzir/series_builder.hpp` — Row-wise series construction
- `tenzir/multi_series_builder.hpp` — Heterogeneous data building
- `tenzir/data_builder.hpp` — Lower-level data construction

# Type Conversion

Converting between types using the `convert(src, dst)` pattern.

## Overview

Tenzir uses explicit `convert()` functions for type-to-type conversion. This
pattern provides a uniform interface for transforming values between different
representations, such as parsing configuration data into typed structs.

## Conversion Pattern

Use explicit `convert()` functions with source and destination parameters:

```cpp
// In header (e.g., my_config.hpp)
auto convert(const data& src, my_config& dst) -> bool;

// In source file (e.g., my_config.cpp)
auto convert(const data& src, my_config& dst) -> bool {
  const auto* rec = try_as<record>(&src);
  if (! rec) {
    return false;
  }
  dst.some_field = get_or(*rec, "some-field", dst.some_field);
  // ... convert remaining fields
  return true;
}
```

## Implementation Pattern

1. Declare the function in the header alongside the struct
2. Implement in the corresponding source file
3. Validate input types before accessing fields
4. Use `get_or()` for optional fields with defaults
5. Use `get_if<T>()` for optional fields without defaults
6. Return success/failure indicator appropriate for your error type

For nested structures, create helper functions:

```cpp
namespace {

auto convert_rule(const data& src, my_config::rule& dst) -> bool {
  const auto* rec = try_as<record>(&src);
  if (! rec) {
    return false;
  }
  // ... convert fields
  return true;
}

} // namespace
```

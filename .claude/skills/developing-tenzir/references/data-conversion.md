# Data Conversion

Converting from `data` (parsed YAML/JSON) to C++ structs.

## Overview

Tenzir configuration files are parsed into `tenzir::data` values (records,
lists, primitives). These must be converted to typed C++ structs for use in the
codebase.

## Generic Conversion

The generic mechanism in `tenzir/concept/convertible/data.hpp` uses template
metaprogramming to automatically convert `data` to any struct with a
`caf::inspect()` overload:

```cpp
#include "tenzir/concept/convertible/data.hpp"

auto result = convert(yaml_data, my_config);
```

This is convenient but has significant compile-time cost due to heavy template
instantiation.

## Targeted Conversion

For performance-critical code paths, use explicit conversion functions:

```cpp
// In header (e.g., index_config.hpp)
caf::error convert(const data& src, index_config& dst);

// In source file (e.g., index_config.cpp)
caf::error convert(const data& src, index_config& dst) {
  const auto* rec = try_as<record>(&src);
  if (! rec) {
    return caf::make_error(ec::convert_error,
                           "expected record for index_config conversion");
  }
  dst.default_fp_rate = get_or(*rec, "default-fp-rate", dst.default_fp_rate);
  // ... convert remaining fields
  return caf::none;
}
```

## When to Use Targeted Conversion

Prefer explicit conversion functions when:

- **Hot paths**: Code instantiated in many translation units
- **Configuration loading**: Types like `index_config`, `concepts_map`,
  `web::configuration`
- **Compile time matters**: The generic mechanism pulls in many headers and
  generates substantial template code

Use the generic mechanism for:

- One-off conversions in isolated code
- Rapid prototyping
- Types with many nested fields where manual conversion is error-prone

## Implementation Pattern

1. Declare the function in the header alongside the struct
2. Implement in the corresponding source file
3. Use `try_as<record>()` to validate the input type
4. Use `get_or()` for optional fields with defaults
5. Use `get_if<T>()` for optional fields without defaults
6. Return `caf::none` on success, `caf::make_error()` on failure

For nested structures, create helper functions:

```cpp
namespace {

auto convert_rule(const data& src, index_config::rule& dst) -> caf::error {
  const auto* rec = try_as<record>(&src);
  if (! rec) {
    return caf::make_error(ec::convert_error, "expected record for rule");
  }
  // ... convert fields
  return caf::none;
}

} // namespace
```

## Testing Requirements

Error path tests are mandatory. Verify that malformed input produces appropriate
errors:

```cpp
TEST(index_config conversion rejects non-record) {
  index_config cfg;
  auto err = convert(data{42}, cfg);
  CHECK(err);
}
```

## Examples

Existing implementations to reference:

- `libtenzir/src/index_config.cpp` - Nested rules with lists
- `libtenzir/src/concepts.cpp` - Map-based configuration
- `plugins/web/src/configuration.cpp` - Plugin configuration

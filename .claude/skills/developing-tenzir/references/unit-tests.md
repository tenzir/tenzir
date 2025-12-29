# Unit Tests

Testing conventions for the Tenzir C++ codebase.

## File Organization

Mirror the component path: `tenzir/detail/feature.hpp` tests go in
`test/detail/feature.cpp`.

## Include Order

1. `#define SUITE name` and unit under test
2. `"test.hpp"` framework
3. Standard library
4. CAF headers
5. Tenzir headers

## Structure

```cpp
#define SUITE foo

#include "tenzir/foo.hpp"

#include "test.hpp"

#include <iostream>

#include "tenzir/bar.hpp"

using namespace tenzir;

namespace {

struct fixture {
  fixture() { /* setup */ }
  ~fixture() { /* teardown */ }
  int context = 42;
};

} // namespace

FIXTURE_SCOPE(foo_tests, fixture)

TEST(construction) {
  MESSAGE("phase description");
  foo x;
  CHECK_EQUAL(x.value(), context);
}

FIXTURE_SCOPE_END()
```

## Guidelines

- Every new feature must include unit tests
- Use fixtures to set up test environment
- Use `MESSAGE()` to describe test phases
- Use `CHECK_EQUAL()`, `CHECK()`, `REQUIRE()` for assertions

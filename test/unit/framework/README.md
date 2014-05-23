VAST Unit Testing Framework
===========================

The **unit** framework offers a simple API to write unit tests.

Concepts
--------

- A **check** represents a single verification of boolean operation.
- A **test** contains one or more checks.
- A **suite** groups tests together.

Example
-------

```cpp
#include "framework/unit.h"

SUITE("core")

TEST("multiply")
{
  REQUIRE(0 * 1 == 0);
  CHECK(42 + 42 == 84);
}
TEST("divide")
{
  NEED(0 / 1 == 0);
  CHECK(1 / 1 == 0);  // fails
}
```

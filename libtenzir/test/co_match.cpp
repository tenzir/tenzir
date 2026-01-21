#include "tenzir/test/test.hpp"
#include "tenzir/co_match.hpp"

namespace tenzir {

TEST("simple") {
  auto v = variant<int, std::string>{42};
  auto result = co_match(
    v,
    [](auto x) {
      return x;
    },
    [](int x) {
      return x;
    },
    [&](std::string) {
      fail("bad type");
      return 42;
    });
  check_eq(result, 42);
}

} // namespace tenzir

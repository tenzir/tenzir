#include "tenzir/co_match.hpp"

#include "tenzir/test/test.hpp"

namespace tenzir {

TEST("monostate variant") {
  auto v = variant<std::monostate>{};
  auto result = co_match(v, [](std::monostate x) {
    return x;
  });
  static_assert(std::same_as<decltype(result), std::monostate>);
}

TEST("move variant") {
  auto v = variant<std::unique_ptr<int>>{std::make_unique<int>(42)};
  auto result = co_match(std::move(v), [](std::unique_ptr<int> x) {
    return x;
  });
  check_eq(*result, 42);
}

TEST("ref variant") {
  auto v = variant<std::unique_ptr<int>>{std::make_unique<int>(42)};
  auto result = co_match(v, [](std::unique_ptr<int>& x) {
    return *x;
  });
  check_eq(result, 42);
}

TEST("simple variant") {
  auto v = variant<int, std::string>{42};
  auto result = co_match(
    v,
    [](int x) {
      return x;
    },
    [&](std::string) {
      fail("bad type");
      return 0;
    });
  check_eq(result, 42);
}

TEST("convertible variant") {
  auto v = variant<int, double>{42};
  auto result = co_match(
    v,
    [&](int x) {
      return x;
    },
    [&](double) {
      fail("bad type");
      return 0;
    });
  check_eq(result, 42);
}

TEST("multi variant") {
  auto v = variant<int, std::string>{42};
  auto w = variant<int, double>{42};
  auto result = co_match(
    std::tie(v, w),
    [&](int x, int y) {
      return x + y;
    },
    [&](auto, auto) {
      fail("bad type");
      return 0;
    });
  check_eq(result, 84);
}

TEST("multi move variant") {
  auto v
    = variant<std::unique_ptr<int>, std::string>{std::make_unique<int>(42)};
  auto w = variant<std::unique_ptr<int>, double>{std::make_unique<int>(42)};
  auto result = co_match(
    std::forward_as_tuple(v, std::move(w)),
    [&](std::unique_ptr<int>& x, std::unique_ptr<int> y) {
      return *x + *y;
    },
    [&](auto&&, auto&&) {
      fail("bad type");
      return 0;
    });
  check_eq(result, 84);
}

} // namespace tenzir

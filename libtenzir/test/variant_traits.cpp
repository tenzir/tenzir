//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/variant_traits.hpp"

#include "tenzir/test/test.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/type.hpp"

namespace tenzir {

TEST(try_as) {
  auto v = std::variant<int, std::unique_ptr<int>>{std::make_unique<int>(42)};
  auto ptr = try_as<std::unique_ptr<int>>(&v);
  REQUIRE(ptr);
  REQUIRE(*ptr);
  REQUIRE(**ptr == 42);
}

TEST(as) {
  auto v = std::variant<int, std::unique_ptr<int>>{std::make_unique<int>(42)};
  auto& ref = as<std::unique_ptr<int>>(v);
  REQUIRE(ref);
  REQUIRE(*ref == 42);
  REQUIRE(*as<std::unique_ptr<int>>(v) == 42);
}

TEST(as move) {
  auto v = std::variant<int, std::unique_ptr<int>>{std::make_unique<int>(42)};
  auto ref = as<std::unique_ptr<int>>(std::move(v));
  REQUIRE(ref);
  REQUIRE(*ref == 42);
  REQUIRE(not as<std::unique_ptr<int>>(v));
}

TEST(as const ref) {
  const auto v = std::variant<int>{42};
  auto& x = as<int>(v);
  REQUIRE(x == 42);
  static_assert(std::same_as<decltype(x), const int&>);
}

TEST(try_as const ref) {
  const auto v = std::variant<int>{42};
  auto x = try_as<int>(v);
  REQUIRE(x);
  REQUIRE(*x == 42);
  static_assert(std::same_as<decltype(x), const int*>);
}

TEST(try_as const ptr) {
  const auto v = std::variant<int>{42};
  auto x = try_as<int>(&v);
  REQUIRE(x);
  REQUIRE(*x == 42);
  static_assert(std::same_as<decltype(x), const int*>);
}

TEST(match) {
  auto v = std::variant<int, std::unique_ptr<int>>{std::make_unique<int>(42)};
  auto& result = match(
    v,
    [](int) -> std::unique_ptr<int>& {
      FAIL("should not be here");
    },
    [](std::unique_ptr<int>& x) -> std::unique_ptr<int>& {
      return x;
    });
  REQUIRE(result);
  REQUIRE(*result == 42);
  REQUIRE(*as<std::unique_ptr<int>>(v) == 42);
}

TEST(match const ref) {
  auto v = std::variant<int, std::unique_ptr<int>>{std::make_unique<int>(42)};
// This `match` yields a `const unique_ptr&`, which correctly refers to the
// value in `v`. For some reason GCC incorrectly diagnoses this as dangling.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdangling-reference"
  auto& result = match(
    std::as_const(v),
    [](int) -> const std::unique_ptr<int>& {
      FAIL("should not be here");
    },
    []<class T>(T&& x) -> const std::unique_ptr<int>& {
      static_assert(std::same_as<T, const std::unique_ptr<int>&>);
      return x;
    });
#pragma GCC diagnostic pop
  REQUIRE(result);
  REQUIRE(*result == 42);
  REQUIRE(*as<std::unique_ptr<int>>(v) == 42);
}

TEST(match move) {
  auto v = std::variant<int, std::unique_ptr<int>>{std::make_unique<int>(42)};
  auto moved = match(
    std::move(v),
    [](int) -> std::unique_ptr<int> {
      FAIL("should not be here");
    },
    [](std::unique_ptr<int> ptr) {
      return ptr;
    });
  REQUIRE(moved);
  REQUIRE(*moved == 42);
  REQUIRE(not as<std::unique_ptr<int>>(v));
}

TEST(match move closure) {
  auto v = std::variant<int, double>{42};
  auto result = match(
    v,
    [x = std::make_unique<int>(43)](int y) {
      return *x + y;
    },
    [](double) {
      FAIL("unexpected type");
      return 0;
    });
  REQUIRE(result == 85);
}

TEST(match null type) {
  auto ty = type{};
  auto is_null = match(
    ty,
    [](null_type) {
      return true;
    },
    [](auto&&) {
      return false;
    });
  REQUIRE(is_null);
}

TEST(match int64 type) {
  auto ty = type{int64_type{}};
  auto is_int64 = match(
    ty,
    [](int64_type) {
      return true;
    },
    [](auto&&) {
      return false;
    });
  REQUIRE(is_int64);
}

TEST(match ip type) {
  auto ty = type{ip_type{}};
  auto is_ip = match(
    ty,
    [](ip_type&) {
      return true;
    },
    []<class T>(T&&) {
      FAIL("unexpected type", typeid(T).name());
      return false;
    });
  REQUIRE(is_ip);
}

TEST(match null array) {
  auto array = arrow::NullArray{42};
  auto length = match(
    static_cast<arrow::Array&>(array),
    [](arrow::NullArray& x) {
      return x.length();
    },
    [](auto&) {
      return int64_t{0};
    });
  REQUIRE(length == 42);
}

TEST(type modification through match) {
  auto ty = type{list_type{int64_type{}}};
  match(
    ty,
    [](list_type& ty) {
      ty = list_type{string_type{}};
    },
    [](auto&) {});
  REQUIRE(as<list_type>(ty).value_type().kind().is<string_type>());
}

TEST(expression) {
  auto expr = ast::expression{
    ast::root_field{ast::identifier{"test", location::unknown}}};
  REQUIRE(try_as<ast::root_field>(&expr));
  REQUIRE(not try_as<ast::this_>(&expr));
  as<ast::root_field>(expr).ident.name = "okay";
  match(
    std::move(expr),
    [](ast::root_field&& x) {
      REQUIRE(x.ident.name == "okay");
    },
    [](auto&&) {
      FAIL("unreachable");
    });
}

} // namespace tenzir

//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/variant_traits.hpp"

#include "tenzir/test/test.hpp"

namespace tenzir {

TEST(try_cast) {
  auto v = std::variant<int, std::unique_ptr<int>>{std::make_unique<int>(42)};
  auto ptr = cast_if<std::unique_ptr<int>>(&v);
  REQUIRE(ptr);
  REQUIRE(*ptr);
  REQUIRE(**ptr == 42);
}

TEST(cast) {
  auto v = std::variant<int, std::unique_ptr<int>>{std::make_unique<int>(42)};
  auto& ref = cast<std::unique_ptr<int>>(v);
  REQUIRE(ref);
  REQUIRE(*ref == 42);
  REQUIRE(*cast<std::unique_ptr<int>>(v) == 42);
}

TEST(cast move) {
  auto v = std::variant<int, std::unique_ptr<int>>{std::make_unique<int>(42)};
  auto ref = cast<std::unique_ptr<int>>(std::move(v));
  REQUIRE(ref);
  REQUIRE(*ref == 42);
  REQUIRE(not cast<std::unique_ptr<int>>(v));
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
  REQUIRE(*cast<std::unique_ptr<int>>(v) == 42);
}

TEST(match const ref) {
  auto v = std::variant<int, std::unique_ptr<int>>{std::make_unique<int>(42)};
  auto& result = match(
    std::as_const(v),
    [](int) -> const std::unique_ptr<int>& {
      FAIL("should not be here");
    },
    []<class T>(T&& x) -> const std::unique_ptr<int>& {
      static_assert(std::same_as<T, const std::unique_ptr<int>&>);
      return x;
    });
  REQUIRE(result);
  REQUIRE(*result == 42);
  REQUIRE(*cast<std::unique_ptr<int>>(v) == 42);
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
  REQUIRE(not cast<std::unique_ptr<int>>(v));
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
  REQUIRE(cast<list_type>(ty).value_type().kind().is<string_type>());
}

TEST(expression) {
  auto expr = ast::expression{
    ast::root_field{ast::identifier{"test", location::unknown}}};
  REQUIRE(cast_if<ast::root_field>(&expr));
  REQUIRE(not cast_if<ast::this_>(&expr));
  cast<ast::root_field>(expr).ident.name = "okay";
  match(
    std::move(expr),
    [](ast::root_field&& x) {
      REQUIRE(x.ident.name == "okay");
    },
    [](auto&&) {
      FAIL("unreachable");
    });
}

// void test() {
//   auto ty = type{};
//   match(
//     ty, []<concrete_type T>(T& x) {}, []<basic_type T>(T& x) {});

//   auto std_var = std::variant<int, double>{};
//   auto caf_var = caf::variant<int, double>{};

//   constexpr auto constexpr_test = std::invoke([] {
//     auto var = std::variant<int, double>{5.0};
//     match(
//       var,
//       [](int x) {

//       },
//       [](double x) {

//       });
//     return 9.5;
//   });
//   auto got = get<double>(std_var);
//   auto ptr = get_if<double>(&std_var);

//   static_assert(constexpr_test == 9.5);

// void foo() {
//   auto xyz1 = match(
//     caf_var,
//     [](int x) {
//       static_assert(true);
//       return 42;
//     },
//     [](double x) {
//       static_assert(true);
//       return 43;
//     },
//     [](std::monostate x) {
//       static_assert(true);
//       return 44;
//     });
//   auto var = std::variant<double>{};
//   auto xyz2 = match2(var)(
//     [](int x) {
//       static_assert(true);
//     },
//     [](double x) {},
//     [](std::monostate x) {
//       static_assert(true);
//     });

//   auto xyz3 = match3(
//     [](int x) {
//       static_assert(true);
//     },
//     [](double x) {},
//     [](std::monostate x) {
//       static_assert(true);
//     })(var);

//   auto xyz = match4{
//     [](int x) {
//       static_assert(true);
//     },
//     [](double x) {},
//     [](std::monostate x) {
//       static_assert(true);
//     },
//   }(var);
// }

} // namespace tenzir

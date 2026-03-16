//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/option.hpp"

#include "tenzir/test/test.hpp"

#include <string>

namespace tenzir {

// -- static assertions --------------------------------------------------------

static_assert(not std::is_copy_constructible_v<Option<std::unique_ptr<int>>>);
static_assert(std::is_copy_constructible_v<Option<int>>);
static_assert(std::is_move_constructible_v<Option<int>>);
static_assert(std::is_default_constructible_v<Option<int>>);
static_assert(has_variant_traits<Option<int>>);
static_assert(has_variant_traits<Option<std::string>>);

// -- construction -------------------------------------------------------------

TEST("default construction is none") {
  auto opt = Option<int>{};
  CHECK(opt.is_none());
  CHECK(not opt.is_some());
  CHECK(not static_cast<bool>(opt));
}

TEST("construction from None") {
  auto opt = Option<int>{None{}};
  CHECK(opt.is_none());
}

TEST("construction from value") {
  auto opt = Option<int>{42};
  CHECK(opt.is_some());
  CHECK_EQUAL(*opt, 42);
}

TEST("implicit construction from value") {
  auto opt = Option<int>{42};
  REQUIRE(opt.is_some());
  CHECK_EQUAL(*opt, 42);
}

TEST("construction from std::optional with value") {
  auto sopt = std::optional<int>{10};
  auto opt = Option<int>{sopt};
  REQUIRE(opt.is_some());
  CHECK_EQUAL(*opt, 10);
}

TEST("construction from empty std::optional") {
  auto sopt = std::optional<int>{};
  auto opt = Option<int>{sopt};
  CHECK(opt.is_none());
}

TEST("CTAD deduces value type") {
  auto opt = Option{42};
  REQUIRE(opt.is_some());
  CHECK_EQUAL(*opt, 42);
  static_assert(std::same_as<decltype(opt), Option<int>>);
}

TEST("construction with move-only type") {
  auto opt = Option<std::unique_ptr<int>>{std::make_unique<int>(42)};
  REQUIRE(opt.is_some());
  CHECK_EQUAL(**opt, 42);
}

// -- reference option ---------------------------------------------------------

TEST("reference option from lvalue") {
  auto val = 42;
  auto opt = Option<int&>{val};
  REQUIRE(opt.is_some());
  CHECK_EQUAL(*opt, 42);
}

TEST("reference option mutation") {
  auto val = 42;
  auto opt = Option<int&>{val};
  *opt = 100;
  CHECK_EQUAL(val, 100);
}

TEST("reference option default is none") {
  auto opt = Option<int&>{};
  CHECK(opt.is_none());
}

// -- assignment ---------------------------------------------------------------

TEST("assignment from None resets") {
  auto opt = Option<int>{42};
  REQUIRE(opt.is_some());
  opt = None{};
  CHECK(opt.is_none());
}

// -- observers ----------------------------------------------------------------

TEST("is_some_and with matching predicate") {
  auto opt = Option<int>{42};
  CHECK(opt.is_some_and([](int const& x) {
    return x == 42;
  }));
}

TEST("is_some_and with non-matching predicate") {
  auto opt = Option<int>{42};
  CHECK(not opt.is_some_and([](int const& x) {
    return x == 0;
  }));
}

TEST("is_some_and on none") {
  auto opt = Option<int>{};
  CHECK(not opt.is_some_and([](int const&) {
    return true;
  }));
}

// -- checked access -----------------------------------------------------------

TEST("operator* on some") {
  auto opt = Option<int>{42};
  CHECK_EQUAL(*opt, 42);
}

TEST("operator* panics on none") {
  auto opt = Option<int>{};
  try {
    (void)*opt;
  } catch (panic_exception const&) {
    return;
  }
  FAIL("expected panic");
}

TEST("operator-> on some") {
  auto opt = Option<std::string>{"hello"};
  CHECK_EQUAL(opt->size(), size_t{5});
}

TEST("operator-> panics on none") {
  auto opt = Option<std::string>{};
  try {
    (void)opt->size();
  } catch (panic_exception const&) {
    return;
  }
  FAIL("expected panic");
}

TEST("unwrap on some") {
  auto opt = Option<int>{42};
  CHECK_EQUAL(opt.unwrap(), 42);
}

TEST("unwrap panics on none") {
  auto opt = Option<int>{};
  try {
    (void)opt.unwrap();
  } catch (panic_exception const&) {
    return;
  }
  FAIL("expected panic");
}

TEST("expect on some") {
  auto opt = Option<int>{42};
  CHECK_EQUAL(opt.expect("should have value"), 42);
}

TEST("expect panics with message on none") {
  auto opt = Option<int>{};
  try {
    (void)opt.expect("custom error message");
  } catch (panic_exception const& e) {
    CHECK(std::string_view{e.what()}.find("custom error message")
          != std::string_view::npos);
    return;
  }
  FAIL("expected panic");
}

TEST("unwrap moves value out") {
  auto opt = Option<std::unique_ptr<int>>{std::make_unique<int>(42)};
  auto val = std::move(opt).unwrap();
  CHECK_EQUAL(*val, 42);
}

// -- unchecked access ---------------------------------------------------------

TEST("unwrap_unchecked on some") {
  auto opt = Option<int>{42};
  CHECK_EQUAL(opt.unwrap_unchecked(), 42);
}

// -- unwrapping with fallback -------------------------------------------------

TEST("unwrap_or on some returns value") {
  auto opt = Option<int>{42};
  CHECK_EQUAL(opt.unwrap_or(99), 42);
}

TEST("unwrap_or on none returns fallback") {
  auto opt = Option<int>{};
  CHECK_EQUAL(opt.unwrap_or(99), 99);
}

TEST("unwrap_or_else on some returns value") {
  auto opt = Option<int>{42};
  CHECK_EQUAL(opt.unwrap_or_else([] {
    return 99;
  }),
              42);
}

TEST("unwrap_or_else on none calls factory") {
  auto opt = Option<int>{};
  CHECK_EQUAL(opt.unwrap_or_else([] {
    return 99;
  }),
              99);
}

TEST("unwrap_or_default on some returns value") {
  auto opt = Option<int>{42};
  CHECK_EQUAL(opt.unwrap_or_default(), 42);
}

TEST("unwrap_or_default on none returns default") {
  auto opt = Option<int>{};
  CHECK_EQUAL(opt.unwrap_or_default(), 0);
}

// -- transformations ----------------------------------------------------------

TEST("map on some") {
  auto opt = Option<int>{21};
  auto result = opt.map([](int x) {
    return x * 2;
  });
  REQUIRE(result.is_some());
  CHECK_EQUAL(*result, 42);
}

TEST("map on none") {
  auto opt = Option<int>{};
  auto result = opt.map([](int x) {
    return x * 2;
  });
  CHECK(result.is_none());
}

TEST("map changes type") {
  auto opt = Option<int>{42};
  auto result = opt.map([](int x) {
    return std::to_string(x);
  });
  static_assert(std::same_as<decltype(result), Option<std::string>>);
  REQUIRE(result.is_some());
  CHECK_EQUAL(*result, "42");
}

TEST("and_then on some returns new option") {
  auto opt = Option<int>{42};
  auto result = opt.and_then([](int x) -> Option<std::string> {
    return std::to_string(x);
  });
  REQUIRE(result.is_some());
  CHECK_EQUAL(*result, "42");
}

TEST("and_then on some can return none") {
  auto opt = Option<int>{-1};
  auto result = opt.and_then([](int x) -> Option<int> {
    if (x > 0) {
      return x;
    }
    return None{};
  });
  CHECK(result.is_none());
}

TEST("and_then on none") {
  auto opt = Option<int>{};
  auto result = opt.and_then([](int x) -> Option<int> {
    return x * 2;
  });
  CHECK(result.is_none());
}

TEST("or_else on some returns self") {
  auto opt = Option<int>{42};
  auto result = opt.or_else([]() -> Option<int> {
    return 99;
  });
  REQUIRE(result.is_some());
  CHECK_EQUAL(*result, 42);
}

TEST("or_else on none calls factory") {
  auto opt = Option<int>{};
  auto result = opt.or_else([]() -> Option<int> {
    return 99;
  });
  REQUIRE(result.is_some());
  CHECK_EQUAL(*result, 99);
}

TEST("filter keeps matching value") {
  auto opt = Option<int>{42};
  auto result = opt.filter([](int const& x) {
    return x > 0;
  });
  REQUIRE(result.is_some());
  CHECK_EQUAL(*result, 42);
}

TEST("filter removes non-matching value") {
  auto opt = Option<int>{-1};
  auto result = opt.filter([](int const& x) {
    return x > 0;
  });
  CHECK(result.is_none());
}

TEST("filter on none") {
  auto opt = Option<int>{};
  auto result = opt.filter([](int const&) {
    return true;
  });
  CHECK(result.is_none());
}

// -- combinators --------------------------------------------------------------

TEST("flatten nested option with value") {
  auto nested = Option<Option<int>>{Option<int>{42}};
  auto flat = nested.flatten();
  REQUIRE(flat.is_some());
  CHECK_EQUAL(*flat, 42);
}

TEST("flatten nested option with inner none") {
  auto nested = Option<Option<int>>{Option<int>{}};
  auto flat = nested.flatten();
  CHECK(flat.is_none());
}

TEST("flatten nested option with outer none") {
  auto nested = Option<Option<int>>{};
  auto flat = nested.flatten();
  CHECK(flat.is_none());
}

TEST("zip two some values") {
  auto a = Option<int>{1};
  auto b = Option<double>{2.0};
  auto result = a.zip(b);
  REQUIRE(result.is_some());
  CHECK_EQUAL(result->first, 1);
  CHECK_EQUAL(result->second, 2.0);
}

TEST("zip with first none") {
  auto a = Option<int>{};
  auto b = Option<double>{2.0};
  auto result = a.zip(b);
  CHECK(result.is_none());
}

TEST("zip with second none") {
  auto a = Option<int>{1};
  auto b = Option<double>{};
  auto result = a.zip(b);
  CHECK(result.is_none());
}

// -- comparison ---------------------------------------------------------------

TEST("equal options") {
  CHECK(Option<int>{42} == Option<int>{42});
  CHECK(not(Option<int>{1} == Option<int>{2}));
  CHECK(Option<int>{} == Option<int>{});
  CHECK(not(Option<int>{42} == Option<int>{}));
}

TEST("option equals value") {
  CHECK(Option<int>{42} == 42);
  CHECK(not(Option<int>{42} == 99));
  CHECK(not(Option<int>{} == 42));
}

TEST("option equals none") {
  CHECK(not(Option<int>{42} == None{}));
  CHECK(Option<int>{} == None{});
}

TEST("reversed comparisons") {
  CHECK(None{} == Option<int>{});
  CHECK(42 == Option<int>{42});
}

TEST("none equals none") {
  CHECK(None{} == None{});
}

TEST("option ordering") {
  CHECK(Option<int>{1} < Option<int>{2});
  CHECK(Option<int>{} < Option<int>{1});
  CHECK(Option<int>{1} > None{});
}

// -- variant_traits / match ---------------------------------------------------

TEST("match on some") {
  auto opt = Option<int>{42};
  auto result = match(
    opt,
    [](int x) {
      return x;
    },
    [](None) {
      return -1;
    });
  CHECK_EQUAL(result, 42);
}

TEST("match on none") {
  auto opt = Option<int>{};
  auto result = match(
    opt,
    [](int) {
      return false;
    },
    [](None) {
      return true;
    });
  CHECK(result);
}

TEST("match moves value") {
  auto opt = Option<std::unique_ptr<int>>{std::make_unique<int>(42)};
  auto result = match(
    std::move(opt),
    [](std::unique_ptr<int> ptr) {
      return *ptr;
    },
    [](None) {
      return -1;
    });
  CHECK_EQUAL(result, 42);
}

TEST("match with single lambda") {
  auto opt = Option<int>{42};
  auto result = match(opt, [](auto x) -> int {
    if constexpr (std::same_as<decltype(x), int>) {
      return x;
    } else {
      return -1;
    }
  });
  CHECK_EQUAL(result, 42);
}

// -- fmt::formatter -----------------------------------------------------------

TEST("format some value") {
  auto opt = Option<int>{42};
  auto str = fmt::format("{}", opt);
  CHECK_EQUAL(str, "Some(42)");
}

TEST("format none value") {
  auto opt = Option<int>{};
  auto str = fmt::format("{}", opt);
  CHECK_EQUAL(str, "None");
}

TEST("format some string") {
  auto opt = Option<std::string>{"hello"};
  auto str = fmt::format("{}", opt);
  CHECK_EQUAL(str, "Some(hello)");
}

// -- const correctness --------------------------------------------------------

TEST("const option access") {
  auto const opt = Option<int>{42};
  CHECK(opt.is_some());
  CHECK_EQUAL(*opt, 42);
  CHECK_EQUAL(opt.unwrap(), 42);
  CHECK_EQUAL(opt.unwrap_or(99), 42);
}

TEST("const option map") {
  auto const opt = Option<int>{42};
  auto result = opt.map([](int const& x) {
    return x * 2;
  });
  REQUIRE(result.is_some());
  CHECK_EQUAL(*result, 84);
}

} // namespace tenzir

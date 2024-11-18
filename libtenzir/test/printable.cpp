//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/concept/printable/core.hpp"
#include "tenzir/concept/printable/numeric.hpp"
#include "tenzir/concept/printable/print.hpp"
#include "tenzir/concept/printable/std/chrono.hpp"
#include "tenzir/concept/printable/stream.hpp"
#include "tenzir/concept/printable/string.hpp"
#include "tenzir/concept/printable/tenzir/data.hpp"
#include "tenzir/concept/printable/tenzir/json.hpp"
#include "tenzir/concept/printable/tenzir/view.hpp"
#include "tenzir/concept/printable/to.hpp"
#include "tenzir/concept/printable/to_string.hpp"
#include "tenzir/detail/escapers.hpp"
#include "tenzir/test/test.hpp"

#include <caf/optional.hpp>

#include <sstream>

using namespace std::string_literals;
using namespace tenzir;
using namespace tenzir::printer_literals;

#define CHECK_TO_STRING(expr, str)                                             \
  {                                                                            \
    auto x = expr;                                                             \
    if constexpr (!std::is_same_v<decltype(x), data>) {                        \
      CHECK_EQUAL(to_string(x), str);                                          \
      CHECK_EQUAL(to_string(make_view(x)), str);                               \
    }                                                                          \
    data data_expr{x};                                                         \
    CHECK_EQUAL(to_string(data_expr), str);                                    \
    CHECK_EQUAL(to_string(make_view(data_expr)), str);                         \
  }

// -- numeric -----------------------------------------------------------------

TEST(signed integers) {
  MESSAGE("no sign");
  auto i = 42;
  std::string str;
  CHECK(printers::integral<int>(str, i));
  CHECK_EQUAL(str, "42");
  MESSAGE("forced sign");
  str.clear();
  CHECK((printers::integral<int, policy::force_sign>(str, i)));
  CHECK_EQUAL(str, "+42");
  MESSAGE("negative sign");
  str.clear();
  int8_t j = -42;
  CHECK(printers::i8(str, j));
  CHECK_EQUAL(str, "-42");
}

TEST(unsigned integers) {
  auto i = 42u;
  std::string str;
  CHECK(printers::integral<unsigned>(str, i));
  CHECK_EQUAL(str, "42");
}

TEST(integral minimum digits) {
  std::string str;
  auto i = 0;
  CHECK((printers::integral<int, policy::plain, 5>(str, i)));
  CHECK_EQUAL(str, "00000");
  str.clear();
  i = 42;
  CHECK((printers::integral<int, policy::force_sign, 4>(str, i)));
  CHECK_EQUAL(str, "+0042");
}

TEST(floating point) {
  std::string str;
  auto d = double{0.0};
  CHECK(printers::real(str, d));
  CHECK_EQUAL(str, "0.0");

  d = 1.0;
  str.clear();
  CHECK(printers::real(str, d));
  CHECK_EQUAL(str, "1.0");

  d = 0.005;
  str.clear();
  CHECK(printers::real(str, d));
  CHECK_EQUAL(str, "0.005");

  d = 123.456;
  str.clear();
  CHECK(printers::real(str, d));
  CHECK_EQUAL(str, "123.456");

  d = -123.456;
  str.clear();
  CHECK(printers::real(str, d));
  CHECK_EQUAL(str, "-123.456");

  d = 123456.1234567890123;
  str.clear();
  CHECK(printers::real(str, d));
  CHECK_EQUAL(str, "123456.123456789");

  d = 123456.1234567890123;
  str.clear();
  CHECK((real_printer<double, 6>{}(str, d)));
  CHECK_EQUAL(str, "123456.123457");

  d = 123456.8888;
  str.clear();
  CHECK((real_printer<double, 0>{}(str, d)));
  CHECK_EQUAL(str, "123457");

  d = 123456.1234567890123;
  str.clear();
  CHECK((real_printer<double, 1>{}(str, d)));
  CHECK_EQUAL(str, "123456.1");

  d = 123456.00123;
  str.clear();
  CHECK((real_printer<double, 6>{}(str, d)));
  CHECK_EQUAL(str, "123456.00123");

  d = 123456.123;
  str.clear();
  CHECK((real_printer<double, 6, 6>{}(str, d)));
  CHECK_EQUAL(str, "123456.123000");
}

// -- string ------------------------------------------------------------------

TEST(string) {
  std::string str;
  CHECK(printers::str(str, "foo"));
  CHECK_EQUAL(str, "foo");
  str.clear();
  CHECK(printers::str(str, "foo"s));
  CHECK_EQUAL(str, "foo");
}

TEST(escape) {
  std::string result;
  auto p = printers::escape(detail::hex_escaper);
  CHECK(p(result, "foo"));
  CHECK_EQUAL(result, R"__(\x66\x6F\x6F)__");
}

// -- core --------------------------------------------------------------------

TEST(literals) {
  std::string str;
  auto p = 42_P << " "_P << 3.14_P;
  CHECK(p(str, unused));
  CHECK_EQUAL(str, "42 3.14");
}

TEST(sequence tuple) {
  auto f = 'f';
  auto oo = "oo";
  auto bar = "bar"s;
  std::string str;
  auto p = printers::any << printers::str << printers::str;
  CHECK(p(str, std::tie(f, oo, bar)));
  CHECK_EQUAL(str, "foobar");
}

TEST(sequence pair) {
  auto f = 'f';
  auto oo = "oo";
  std::string str;
  auto p = printers::any << printers::str;
  CHECK(p(str, std::make_pair(f, oo)));
  CHECK_EQUAL(str, "foo");
}

TEST(choice) {
  using namespace printers;
  auto x = caf::variant<char, bool, int64_t>{true};
  auto p = any | tf | i64;
  std::string str;
  CHECK(p(str, x));
  CHECK_EQUAL(str, "true");
  str.clear();
  x = 'c';
  CHECK(p(str, x));
  CHECK_EQUAL(str, "c");
  str.clear();
  x = int64_t{64};
  CHECK(p(str, x));
  CHECK_EQUAL(str, "64");
}

TEST(kleene) {
  auto xs = std::vector<char>{'f', 'o', 'o'};
  std::string str;
  auto p = *printers::any;
  CHECK(p(str, xs));
  CHECK_EQUAL(str, "foo");
  xs.clear();
  str.clear();
  CHECK(p(str, xs)); // 0 elements are allowed.
}

TEST(plus) {
  auto xs = std::vector<char>{'b', 'a', 'r'};
  std::string str;
  auto p = +printers::any;
  CHECK(p(str, xs));
  CHECK_EQUAL(str, "bar");
  xs.clear();
  str.clear();
  CHECK(!p(str, xs)); // 0 elements are *not* allowed!
}

TEST(list) {
  auto xs = std::vector<int>{1, 2, 4, 8};
  auto p = printers::integral<int> % ' ';
  std::string str;
  CHECK(p(str, xs));
  CHECK_EQUAL(str, "1 2 4 8");
  xs.resize(1);
  str.clear();
  CHECK(p(str, xs));
  CHECK_EQUAL(str, "1");
  xs.clear();
  CHECK(p(str, xs));
}

TEST(optional) {
  std::optional<int> x;
  auto p = -printers::integral<int>;
  std::string str;
  CHECK(p(str, x));
  CHECK(str.empty()); // nothing to see here, move along
  x = 42;
  CHECK(p(str, x));
  CHECK_EQUAL(str, "42");
}

TEST(action) {
  auto flag = false;
  // no args, void result type
  auto p0 = printers::integral<int>->*[&] {
    flag = true;
  };
  std::string str;
  CHECK(p0(str, 42));
  CHECK(flag);
  CHECK_EQUAL(str, "42");
  // one arg, void result type
  auto p1 = printers::integral<int>->*[&](int i) {
    flag = i % 2 == 0;
  };
  str.clear();
  CHECK(p1(str, 8));
  CHECK_EQUAL(str, "8");
  // no args, non-void result type
  auto p2 = printers::integral<int>->*[] {
    return 42;
  };
  str.clear();
  CHECK(p2(str, 7));
  CHECK_EQUAL(str, "42");
  // one arg, non-void result type
  auto p3 = printers::integral<int>->*[](int i) {
    return ++i;
  };
  str.clear();
  CHECK(p3(str, 41));
  CHECK_EQUAL(str, "42");
}

TEST(epsilon) {
  std::string str;
  CHECK(printers::eps(str, "whatever"));
}

TEST(guard) {
  std::string str;
  auto always_false = printers::eps.with([] {
    return false;
  });
  CHECK(!always_false(str, 0));
  auto even = printers::integral<int>.with([](int i) {
    return i % 2 == 0;
  });
  CHECK(str.empty());
  CHECK(!even(str, 41));
  CHECK(str.empty());
  CHECK(even(str, 42));
  CHECK_EQUAL(str, "42");
}

TEST(and) {
  std::string str;
  auto flag = true;
  auto p = &printers::eps.with([&] {
    return flag;
  }) << printers::str;
  CHECK(p(str, "yoda"));
  CHECK_EQUAL(str, "yoda");
  flag = false;
  str.clear();
  CHECK(!p(str, "chewie"));
  CHECK(str.empty());
}

TEST(not ) {
  std::string str;
  auto flag = true;
  auto p = !printers::eps.with([&] {
    return flag;
  }) << printers::str;
  CHECK(!p(str, "yoda"));
  CHECK(str.empty());
  flag = false;
  CHECK(p(str, "chewie"));
  CHECK_EQUAL(str, "chewie");
}

// -- Tenzir types ---------------------------------------------------------------

TEST(data) {
  data r{double{12.21}};
  CHECK_TO_STRING(r, "12.21");
  data b{true};
  CHECK_TO_STRING(b, "true");
  data c{uint64_t{23}};
  CHECK_TO_STRING(c, "23");
  data i{int64_t{42}};
  CHECK_TO_STRING(i, "+42");
  data s{std::string{"foobar"}};
  CHECK_TO_STRING(s, "\"foobar\"");
  data d{duration{512}};
  CHECK_TO_STRING(d, "512.0ns");
  data v{list{r, b, c, i, s, d}};
  CHECK_TO_STRING(v, "[12.21, true, 23, +42, \"foobar\", 512.0ns]");
}

// -- std::chrono types -------------------------------------------------------

TEST(duration) {
  using namespace std::chrono_literals;
  CHECK_TO_STRING(15ns, "15.0ns");
  CHECK_TO_STRING(15'450ns, "15.45us");
  CHECK_TO_STRING(42us, "42.0us");
  CHECK_TO_STRING(42'123us, "42.12ms");
  CHECK_TO_STRING(-7ms, "-7.0ms");
  CHECK_TO_STRING(59s, "59.0s");
  CHECK_TO_STRING(60s, "1.0m");
  CHECK_TO_STRING(-90s, "-1.5m");
  CHECK_TO_STRING(390s, "6.5m");
  CHECK_TO_STRING(-2400h, "-100.0d");
}

TEST(time) {
  using namespace std::chrono_literals;
  CHECK_TO_STRING(tenzir::time{0s}, "1970-01-01T00:00:00.000000");
  CHECK_TO_STRING(tenzir::time{1ms}, "1970-01-01T00:00:00.001000");
  CHECK_TO_STRING(tenzir::time{1us}, "1970-01-01T00:00:00.000001");
  CHECK_TO_STRING(tenzir::time{1ns}, "1970-01-01T00:00:00.000000");
  CHECK_TO_STRING(tenzir::time{999ns}, "1970-01-01T00:00:00.000000");
  CHECK_TO_STRING(tenzir::time{1502658642123456us},
                  "2017-08-13T21:10:42.123456");
}

// -- JSON --------------------------------------------------------------------

template <class Printer, class T>
void check_to_json(Printer& p, const T& value, const char* expected) {
  const auto to_json = [&](auto x) {
    std::string str;
    auto out = std::back_inserter(str);
    REQUIRE(p.print(out, x));
    return str;
  };
  CHECK_EQUAL(to_json(value), expected);
  CHECK_EQUAL(to_json(make_view(value)), expected);
  if constexpr (!std::is_same_v<T, data>) {
    data dx{value};
    CHECK_EQUAL(to_json(dx), expected);
    CHECK_EQUAL(to_json(make_view(dx)), expected);
  }
}

TEST(JSON - omit nulls) {
  auto p = json_printer{json_printer_options{
    .oneline = true,
    .omit_nulls = true,
  }};
  check_to_json(p,
                tenzir::record{{"a", 42u}, {"b", caf::none}, {"c", caf::none}},
                R"__({"a": 42})__");
  check_to_json(p,
                tenzir::record{{"a", tenzir::record{{"b", caf::none}}},
                               {"c", caf::none}},
                R"__({"a": {}})__");
  check_to_json(p,
                tenzir::record{
                  {"a", 42u},
                  {"b", record{{"c", caf::none}, {"d", caf::none}}},
                  {"e", record{{"f", record{{"g", caf::none}}}}},
                },
                R"__({"a": 42, "b": {}, "e": {"f": {}}})__");
}

TEST(JSON - omit empty records) {
  auto p = json_printer{json_printer_options{
    .oneline = true,
    .omit_nulls = true,
    .omit_empty_records = true,
  }};
  check_to_json(p,
                tenzir::record{{"a", 42u}, {"b", caf::none}, {"c", caf::none}},
                R"__({"a": 42})__");
  check_to_json(p,
                tenzir::record{{"a", tenzir::record{{"b", caf::none}}},
                               {"c", caf::none}},
                "{}");
  check_to_json(p,
                tenzir::record{
                  {"a", 42u},
                  {"b", record{{"c", caf::none}, {"d", caf::none}}},
                  {"e", record{{"f", record{{"g", caf::none}}}}},
                },
                R"__({"a": 42})__");
}

TEST(JSON - omit empty lists) {
  {
    auto p = json_printer{json_printer_options{
      .oneline = true,
      .omit_empty_records = true,
      .omit_empty_lists = true,
    }};
    check_to_json(p,
                  tenzir::record{{"a", tenzir::list{}},
                                 {"b", tenzir::list{}},
                                 {"c", caf::none}},
                  R"__({"c": null})__");
    check_to_json(
      p,
      tenzir::list{tenzir::record{{"a", tenzir::record{{"b", caf::none}}},
                                  {"c", caf::none}},
                   tenzir::record{}},
      R"__([{"a": {"b": null}, "c": null}])__");
    check_to_json(
      p,
      tenzir::record{
        {"a", 42u},
        {"b", record{{"c", caf::none}, {"d", caf::none}}},
        {"e", record{{"f", tenzir::list{record{{"g", caf::none}}}}}},
      },
      R"__({"a": 42, "b": {"c": null, "d": null}, "e": {"f": [{"g": null}]}})__");
  }
  {
    auto p = json_printer{json_printer_options{
      .oneline = true,
      .omit_nulls = true,
      .omit_empty_records = true,
      .omit_empty_lists = true,
    }};
    check_to_json(p,
                  tenzir::record{{"a", tenzir::list{}},
                                 {"b", tenzir::list{}},
                                 {"c", caf::none}},
                  "{}");
    check_to_json(
      p,
      tenzir::list{tenzir::record{{"a", tenzir::record{{"b", caf::none}}},
                                  {"c", caf::none}},
                   tenzir::record{}},
      R"__([])__");
    check_to_json(
      p,
      tenzir::record{
        {"a", 42u},
        {"b", record{{"c", caf::none}, {"d", caf::none}}},
        {"e", record{{"f", tenzir::list{record{{"g", caf::none}}}}}},
      },
      R"__({"a": 42})__");
  }
}

TEST(JSON - remove trailing zeroes) {
  auto p = json_printer{json_printer_options{
    .oneline = true,
    .omit_nulls = true,
  }};
  check_to_json(p, 5.0, "5.0");
  check_to_json(p, 5.10, "5.1");
}

// -- API ---------------------------------------------------------------------

TEST(to) {
  auto t = to<std::string>(true);
  REQUIRE(t);
  CHECK(*t == "true");
}

TEST(to_string) {
  auto str = to_string(true);
  CHECK_EQUAL(str, "true");
}

//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#define SUITE printable

#include "vast/test/test.hpp"

#include "vast/concept/printable/core.hpp"
#include "vast/concept/printable/numeric.hpp"
#include "vast/concept/printable/print.hpp"
#include "vast/concept/printable/std/chrono.hpp"
#include "vast/concept/printable/stream.hpp"
#include "vast/concept/printable/string.hpp"
#include "vast/concept/printable/to.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/view.hpp"
#include "vast/detail/escapers.hpp"
#include "vast/fmt_integration.hpp"

#include <caf/optional.hpp>

#include <sstream>

using namespace std::string_literals;
using namespace vast;
using namespace vast::printer_literals;

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
  CHECK(printers::integral<int, policy::force_sign>(str, i));
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
  CHECK(printers::integral<int, policy::plain, 5>(str, i));
  CHECK_EQUAL(str, "00000");
  str.clear();
  i = 42;
  CHECK(printers::integral<int, policy::force_sign, 4>(str, i));
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
  CHECK(real_printer<double, 6>{}(str, d));
  CHECK_EQUAL(str, "123456.123457");

  d = 123456.8888;
  str.clear();
  CHECK(real_printer<double, 0>{}(str, d));
  CHECK_EQUAL(str, "123457");

  d = 123456.1234567890123;
  str.clear();
  CHECK(real_printer<double, 1>{}(str, d));
  CHECK_EQUAL(str, "123456.1");

  d = 123456.00123;
  str.clear();
  CHECK(real_printer<double, 6>{}(str, d));
  CHECK_EQUAL(str, "123456.00123");

  d = 123456.123;
  str.clear();
  CHECK(real_printer<double, 6, 6>{}(str, d));
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
  CHECK_EQUAL(str, "T");
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
  caf::optional<int> x;
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
  auto p0 = printers::integral<int>->*[&] { flag = true; };
  std::string str;
  CHECK(p0(str, 42));
  CHECK(flag);
  CHECK_EQUAL(str, "42");
  // one arg, void result type
  auto p1 = printers::integral<int>->*[&](int i) { flag = i % 2 == 0; };
  str.clear();
  CHECK(p1(str, 8));
  CHECK_EQUAL(str, "8");
  // no args, non-void result type
  auto p2 = printers::integral<int>->*[] { return 42; };
  str.clear();
  CHECK(p2(str, 7));
  CHECK_EQUAL(str, "42");
  // one arg, non-void result type
  auto p3 = printers::integral<int>->*[](int i) { return ++i; };
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
  auto always_false = printers::eps.with([] { return false; });
  CHECK(!always_false(str, 0));
  auto even = printers::integral<int>.with([](int i) { return i % 2 == 0; });
  CHECK(str.empty());
  CHECK(!even(str, 41));
  CHECK(str.empty());
  CHECK(even(str, 42));
  CHECK_EQUAL(str, "42");
}

TEST(and) {
  std::string str;
  auto flag = true;
  auto p = &printers::eps.with([&] { return flag; }) << printers::str;
  CHECK(p(str, "yoda"));
  CHECK_EQUAL(str, "yoda");
  flag = false;
  str.clear();
  CHECK(!p(str, "chewie"));
  CHECK(str.empty());
}

TEST(not) {
  std::string str;
  auto flag = true;
  auto p = !printers::eps.with([&] { return flag; }) << printers::str;
  CHECK(!p(str, "yoda"));
  CHECK(str.empty());
  flag = false;
  CHECK(p(str, "chewie"));
  CHECK_EQUAL(str, "chewie");
}

// -- VAST types ---------------------------------------------------------------

TEST(data) {
  data r{real{12.21}};
  CHECK_TO_STRING(r, "12.21");
  data b{true};
  CHECK_TO_STRING(b, "T");
  data c{count{23}};
  CHECK_TO_STRING(c, "23");
  data i{integer{42}};
  CHECK_TO_STRING(i, "42");
  data s{std::string{"foobar"}};
  CHECK_TO_STRING(s, "\"foobar\"");
  data d{duration{512}};
  CHECK_TO_STRING(d, "512.0ns");
  data v{list{r, b, c, i, s, d}};
  CHECK_TO_STRING(v, "[12.21, T, 23, 42, \"foobar\", 512.0ns]");
}

// -- std::chrono types -------------------------------------------------------

TEST(duration) {
  using namespace std::chrono_literals;
  CHECK_TO_STRING(data{15ns}, "15.0ns");
  CHECK_TO_STRING(data{15'450ns}, "15.45us");
  CHECK_TO_STRING(data{42us}, "42.0us");
  CHECK_TO_STRING(data{42'123us}, "42.12ms");
  CHECK_TO_STRING(data{-7ms}, "-7.0ms");
  CHECK_TO_STRING(data{59s}, "59.0s");
  CHECK_TO_STRING(data{60s}, "1.0m");
  CHECK_TO_STRING(data{-90s}, "-1.5m");
  CHECK_TO_STRING(data{390s}, "6.5m");
  CHECK_TO_STRING(data{-2400h}, "-100.0d");
}

TEST(time) {
  using namespace std::chrono_literals;
  CHECK_TO_STRING(vast::time{0s}, "1970-01-01T00:00:00");
  CHECK_TO_STRING(vast::time{1ms}, "1970-01-01T00:00:00.001");
  CHECK_TO_STRING(vast::time{1us}, "1970-01-01T00:00:00.000001");
  CHECK_TO_STRING(vast::time{1ns}, "1970-01-01T00:00:00.000000001");
  CHECK_TO_STRING(vast::time{1502658642123456us}, "2017-08-13T21:10:42.123456");
}

// -- API ---------------------------------------------------------------------

TEST(stream) {
  using namespace std::chrono;
  std::ostringstream ss;
  auto x = nanoseconds(42);
  ss << x;
  CHECK_EQUAL(ss.str(), "42.0ns");
}

TEST(to) {
  auto t = to<std::string>(true);
  REQUIRE(t);
  CHECK(*t == "T");
}

TEST(to_string) {
  auto str = to_string(true);
  CHECK_EQUAL(str, "T");
}

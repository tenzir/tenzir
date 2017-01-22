#include <sstream>

#include "vast/concept/printable/core.hpp"
#include "vast/concept/printable/numeric.hpp"
#include "vast/concept/printable/print.hpp"
#include "vast/concept/printable/std/chrono.hpp"
#include "vast/concept/printable/string.hpp"
#include "vast/concept/printable/stream.hpp"
#include "vast/concept/printable/to.hpp"
#include "vast/concept/printable/to_string.hpp"

#define SUITE printable
#include "test.hpp"

using namespace vast;
using namespace std::string_literals;

// -- numeric -----------------------------------------------------------------

TEST(signed integers) {
  auto i = 42;
  std::string str;
  CHECK(printers::integral<int>(str, i));
  CHECK_EQUAL(str, "+42");

  int8_t j = -42;
  str.clear();
  CHECK(printers::i8(str, j));
  CHECK_EQUAL(str, "-42");
}

TEST(unsigned integers) {
  auto i = 42u;
  std::string str;
  CHECK(printers::integral<unsigned>(str, i));
  CHECK_EQUAL(str, "42");
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
  auto x = variant<char, bool, int64_t>{true};
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
  CHECK_EQUAL(str, "+64");
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
  CHECK_EQUAL(str, "+1 +2 +4 +8");
  xs.resize(1);
  str.clear();
  CHECK(p(str, xs));
  CHECK_EQUAL(str, "+1");
  xs.clear();
  CHECK(!p(str, xs)); // need at least one element
}

TEST(optional) {
  optional<int> x;
  auto p = -printers::integral<int>;
  std::string str;
  CHECK(p(str, x));
  CHECK(str.empty()); // nothing to see here, move along
  x = 42;
  CHECK(p(str, x));
  CHECK_EQUAL(str, "+42");
}

TEST(action) {
  auto flag = false;
  // no args, void result type
  auto p0 = printers::integral<int> ->* [&] { flag = true; };
  std::string str;
  CHECK(p0(str, 42));
  CHECK(flag);
  CHECK_EQUAL(str, "+42");
  // one arg, void result type
  auto p1 = printers::integral<int> ->* [&](int i) { flag = i % 2 == 0; };
  str.clear();
  CHECK(p1(str, 8));
  CHECK_EQUAL(str, "+8");
  // no args, non-void result type
  auto p2 = printers::integral<int> ->* [] { return 42; };
  str.clear();
  CHECK(p2(str, 7));
  CHECK_EQUAL(str, "+42");
  // one arg, non-void result type
  auto p3 = printers::integral<int> ->* [](int i) { return ++i; };
  str.clear();
  CHECK(p3(str, 41));
  CHECK_EQUAL(str, "+42");
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
  CHECK_EQUAL(str, "+42");
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

// -- custom type -------------------------------------------------------------

namespace ns {

struct foo {
  int i = 42;
};

} // namespace n

template <>
struct access::printer<ns::foo> : vast::printer<access::printer<ns::foo>> {
  template <typename Iterator>
  bool print(Iterator& out, ns::foo const& x) const {
    using vast::print;
    return print(out, x.i);
  }
};

TEST(custom type) {
  std::string str;
  CHECK(print(std::back_inserter(str), ns::foo{}));
  CHECK_EQUAL(str, "+42");
}

// -- std::chrono -------------------------------------------------------------

TEST(std::chrono) {
  using namespace std::chrono;
  auto ns = nanoseconds(15);
  CHECK_EQUAL(to_string(ns), "+15ns");
  auto us = microseconds(42);
  CHECK_EQUAL(to_string(us), "+42us");
  auto ms = milliseconds(-7);
  CHECK_EQUAL(to_string(ms), "-7ms");
}

// -- API ---------------------------------------------------------------------

TEST(stream) {
  std::ostringstream ss;
  auto x = ns::foo{};
  ss << x;
  CHECK(ss.str() == "+42");
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

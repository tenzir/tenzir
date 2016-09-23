#include <sstream>

#include "vast/concept/printable/core.hpp"
#include "vast/concept/printable/numeric.hpp"
#include "vast/concept/printable/print.hpp"
#include "vast/concept/printable/string.hpp"
#include "vast/concept/printable/stream.hpp"
#include "vast/concept/printable/to.hpp"
#include "vast/concept/printable/to_string.hpp"

#define SUITE printable
#include "test.hpp"

using namespace vast;
using namespace std::string_literals;

TEST(signed integers) {
  auto i = 42;
  std::string str;
  CHECK(printers::integral<int>(str, i));
  CHECK(str == "+42");

  int8_t j = -42;
  str.clear();
  CHECK(printers::i8(str, j));
  CHECK(str == "-42");
}

TEST(unsigned integers) {
  auto i = 42u;
  std::string str;
  CHECK(printers::integral<unsigned>(str, i));
  CHECK(str == "42");
}

TEST(floating point) {
  std::string str;
  auto d = double{0.0};
  CHECK(printers::real(str, d));
  CHECK(str == "0.0000000000");

  str.clear();
  d = 1.0;
  CHECK(printers::real(str, d));
  CHECK(str == "1.0000000000");

  str.clear();
  d = 0.005;
  CHECK(printers::real(str, d));
  CHECK(str == "0.0050000000");

  str.clear();
  d = 123.456;
  CHECK(printers::real(str, d));
  CHECK(str == "123.4560000000");

  str.clear();
  d = -123.456;
  CHECK(printers::real(str, d));
  CHECK(str == "-123.4560000000");

  str.clear();
  d = 123456.1234567890123;
  CHECK(printers::real(str, d));
  CHECK(str == "123456.1234567890");

  str.clear();
  d = 123456.1234567890123;
  CHECK(real_printer<double, 6>{}(str, d));
  CHECK(str == "123456.123457");

  str.clear();
  d = 123456.8888;
  CHECK(real_printer<double, 0>{}(str, d));
  CHECK(str == "123457");

  str.clear();
  d = 123456.1234567890123;
  CHECK(real_printer<double, 1>{}(str, d));
  CHECK(str == "123456.1");

  str.clear();
  d = 123456.00123;
  CHECK(real_printer<double, 6>{}(str, d));
  CHECK(str == "123456.001230");
}

TEST(string) {
  std::string str;
  CHECK(printers::str(str, "foo"));
  CHECK_EQUAL(str, "foo");
  str.clear();
  CHECK(printers::str(str, "foo"s));
  CHECK_EQUAL(str, "foo");
}

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
  CHECK(str == "+42");
}

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
  CHECK(str == "T");
}

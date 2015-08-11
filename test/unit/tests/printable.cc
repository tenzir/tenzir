#include <sstream>

#include "vast/concept/printable/numeric.h"
#include "vast/concept/printable/print.h"
#include "vast/concept/printable/string.h"
#include "vast/concept/printable/stream.h"
#include "vast/concept/printable/to.h"
#include "vast/concept/printable/to_string.h"

#define SUITE printable
#include "test.h"

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
  CHECK(str == "foo");

  str.clear();
  CHECK(printers::str(str, "foo"s));
  CHECK(str == "foo");
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

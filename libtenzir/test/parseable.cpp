//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/concept/parseable/core.hpp"
#include "tenzir/concept/parseable/numeric.hpp"
#include "tenzir/concept/parseable/string.hpp"
#include "tenzir/concept/parseable/tenzir/ip.hpp"
#include "tenzir/concept/parseable/tenzir/offset.hpp"
#include "tenzir/concept/parseable/tenzir/option_set.hpp"
#include "tenzir/concept/parseable/tenzir/si.hpp"
#include "tenzir/concept/parseable/tenzir/time.hpp"
#include "tenzir/concept/parseable/to.hpp"
#include "tenzir/detail/narrow.hpp"
#include "tenzir/ip.hpp"
#include "tenzir/si_literals.hpp"
#include "tenzir/test/test.hpp"

#include <caf/test/dsl.hpp>
#include <fmt/format.h>

#include <array>
#include <map>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

using namespace std::string_literals;
using namespace std::string_view_literals;
using namespace tenzir;
using namespace tenzir::parser_literals;

namespace {

template <class Parser>
auto skip_to_eoi(Parser&& parser) {
  // For range-based input, ignore that EOI may have not been reached.
  return std::forward<Parser>(parser) >> ignore(*parsers::any);
}

} // namespace

// -- core --------------------------------------------------------------------

TEST(choice - LHS and RHS) {
  using namespace parsers;
  auto p = chr{'x'} | i32;
  caf::variant<char, int32_t> x;
  CHECK(p("123", x));
  auto i = caf::get_if<int32_t>(&x);
  REQUIRE(i);
  CHECK_EQUAL(*i, 123);
  CHECK(p("x", x));
  auto c = caf::get_if<char>(&x);
  REQUIRE(c);
  CHECK_EQUAL(*c, 'x');
}

TEST(choice - unused LHS) {
  using namespace parsers;
  auto p = 'x' | i32;
  int32_t i;
  CHECK(p("123", i));
  CHECK_EQUAL(i, 123);
  i = 0;
  CHECK(p("x", i));
  CHECK_EQUAL(i, 0); // didn't mess with i
}

TEST(choice triple) {
  using namespace parsers;
  auto fired = false;
  auto p = chr{'x'} | i32 | eps->*[&] {
    fired = true;
  };
  caf::variant<char, int32_t> x;
  CHECK(skip_to_eoi(p)("foobar", x));
  CHECK(fired);
}

TEST(list) {
  auto p = parsers::alnum % '.';
  std::vector<char> xs;
  std::string str;
  CHECK(p("a.b.c", xs));
  CHECK(p("a.b.c", str));
  CHECK_EQUAL(xs, (std::vector<char>{'a', 'b', 'c'}));
  CHECK_EQUAL(str, "abc");
}

TEST(maybe) {
  using namespace parsers;
  auto maybe_x = ~chr{'x'};
  auto c = 'x';
  auto f = &c;
  auto l = &c + 1;
  char result = 0;
  CHECK(maybe_x(f, l, result));
  CHECK(f == l);
  CHECK(result == 'x');
  c = 'y';
  f = &c;
  result = '\0';
  CHECK(maybe_x(f, l, result));
  CHECK(f == &c);        // Iterator not advanced.
  CHECK(result == '\0'); // Result not modified.
}

TEST(container attribute folding) {
  using namespace parsers;
  auto spaces = *' '_p;
  static_assert(std::is_same_v<decltype(spaces)::attribute, unused_type>,
                "container attribute folding failed");
}

TEST(action) {
  auto make_v4 = [](uint32_t a) {
    return ip::v4(a);
  };
  auto ipv4_addr = parsers::b32be->*make_v4;
  ip x;
  CHECK(ipv4_addr("\x0A\x00\x00\x01", x));
  CHECK_EQUAL(x, unbox(to<ip>("10.0.0.1")));
}

TEST(end of input) {
  auto input = "foo"s;
  CHECK(!parsers::eoi(input));
  input.clear();
  CHECK(parsers::eoi(input));
}

// -- string ------------------------------------------------------------------

TEST(char) {
  using namespace parsers;
  MESSAGE("equality");
  auto character = '.';
  auto f = &character;
  auto l = f + 1;
  char c;
  CHECK(chr{'.'}(f, l, c));
  CHECK(c == character);
  CHECK(f == l);

  MESSAGE("inequality");
  character = 'x';
  f = &character;
  CHECK(!chr{'y'}(f, l, c));
  CHECK(f != l);
}

TEST(char class) {
  using namespace parsers;
  MESSAGE("xdigit");
  auto str = "deadbeef"s;
  auto attr = ""s;
  auto f = str.begin();
  auto l = str.end();
  auto p = +xdigit;
  CHECK(p(f, l, attr));
  CHECK(attr == str);
  CHECK(f == l);

  MESSAGE("xdigit fail");
  str = "deadXbeef"s;
  attr.clear();
  f = str.begin();
  l = str.end();
  CHECK(p(f, l, attr));
  CHECK(attr == "dead");
  CHECK(f == str.begin() + 4);
  CHECK(!p(f, l, attr));
  ++f;
  CHECK(p(f, l, attr));
  CHECK(f == l);
  CHECK(attr == "deadbeef");
}

TEST(literal) {
  std::string_view attr;
  CHECK(parsers::lit{"foo"}("foo", attr));
  CHECK_EQUAL(attr, "foo"sv);
}

TEST(quoted string - empty) {
  std::string attr;
  CHECK(parsers::qstr("''", attr));
  CHECK_EQUAL(attr, "");
}

TEST(quoted string - no escaped chars) {
  std::string attr;
  CHECK(parsers::qstr("'foobar'", attr));
  CHECK_EQUAL(attr, "foobar");
}

TEST(quoted string - escaped char at beginning) {
  std::string attr;
  CHECK(parsers::qstr("'\\'foobar'", attr));
  CHECK_EQUAL(attr, "'foobar");
}

TEST(quoted string - escaped char in middle) {
  std::string attr;
  CHECK(parsers::qstr("'foo\\'bar'", attr));
  CHECK_EQUAL(attr, "foo'bar");
}

TEST(quoted string - escaped char at end) {
  std::string attr;
  CHECK(parsers::qstr("'foobar\\''", attr));
  CHECK_EQUAL(attr, "foobar'");
}

TEST(quoted string - missing trailing quote) {
  std::string attr;
  CHECK(!parsers::qstr("'foobar", attr));
  CHECK_EQUAL(attr, "foobar");
}

TEST(quoted string - missing trailing quote after escaped quote) {
  std::string attr;
  CHECK(!parsers::qstr("'foobar\\'", attr));
  CHECK_EQUAL(attr, "foobar'");
}

TEST(quoted string - trailing quote after escaped escape) {
  std::string attr;
  CHECK(parsers::qstr("'foobar\\\\'", attr));
  CHECK_EQUAL(attr, "foobar\\\\");
}

TEST(symbol table) {
  symbol_table<int> sym{{"foo", 42}, {"bar", 84}, {"foobar", 1337}};
  int i = 0;
  CHECK(sym("foo", i));
  CHECK(i == 42);
  CHECK(sym("bar", i));
  CHECK(i == 84);
  CHECK(sym("foobar", i));
  CHECK(i == 1337);
  i = 0;
  CHECK(!sym("baz", i));
  CHECK(i == 0);
}

TEST(attribute compatibility with string) {
  auto str = "..."s;
  auto attr = ""s;
  auto f = str.begin();
  auto l = str.end();
  auto p = parsers::chr{'.'};

  MESSAGE("char into string");
  CHECK(p(f, l, attr));
  CHECK(attr == ".");
  CHECK(p(f, l, attr));
  CHECK(attr == "..");
  CHECK(p(f, l, attr));
  CHECK(attr == str);
  CHECK(f == l);

  MESSAGE("plus(+)");
  attr.clear();
  f = str.begin();
  auto plus = +p;
  CHECK(plus(f, l, attr));
  CHECK(str == attr);
  CHECK(f == l);

  MESSAGE("kleene (*)");
  attr.clear();
  f = str.begin();
  auto kleene = *p;
  CHECK(kleene(f, l, attr));
  CHECK(str == attr);
  CHECK(f == l);

  MESSAGE("sequence (>>)");
  attr.clear();
  f = str.begin();
  auto seq = p >> p >> p;
  CHECK(seq(f, l, attr));
  CHECK(str == attr);
  CHECK(f == l);
}

TEST(attribute compatibility with pair) {
  using namespace parsers;
  auto str = "xy"s;
  auto attr = ""s;
  auto f = str.begin();
  auto l = str.end();
  auto c = chr{'x'} >> chr{'y'};

  MESSAGE("pair<char, char>");
  std::pair<char, char> p0;
  CHECK(c(f, l, p0));
  CHECK(p0.first == 'x');
  CHECK(p0.second == 'y');

  MESSAGE("pair<string, string>");
  f = str.begin();
  std::pair<std::string, std::string> p1;
  CHECK(c(f, l, p1));
  CHECK(p1.first == "x");
  CHECK(p1.second == "y");
}

TEST(attribute compatibility with map) {
  using namespace parsers;
  auto str = "a->x,b->y,c->z"s;
  auto f = str.begin();
  auto l = str.end();
  std::map<char, char> map;
  auto p = (any >> "->" >> any) % ',';
  CHECK(p(f, l, map));
  CHECK(f == l);
  CHECK(map['a'] == 'x');
  CHECK(map['b'] == 'y');
  CHECK(map['c'] == 'z');
}

TEST(attribute compatibility with string sequences) {
  using namespace parsers;
  auto p = alpha >> '-' >> alpha >> '-' >> alpha;
  std::string str;
  CHECK(p("x-y-z", str));
  CHECK(str == "xyz");
}

TEST(polymorphic) {
  using namespace parsers;
  auto p = type_erased_parser<std::string::iterator>{'a'_p};
  MESSAGE("from construction");
  auto str = "a"s;
  auto f = str.begin();
  auto l = str.end();
  CHECK(p(f, l, unused));
  CHECK_EQUAL(f, l);
  MESSAGE("extended with matching type");
  p = p >> ',';
  p = p >> 'b';
  str += ",b"s;
  f = str.begin();
  l = str.end();
  CHECK(p(f, l, unused));
  CHECK_EQUAL(f, l);
  MESSAGE("extended with different type");
  p = p >> "hello!";
  str += "hello!";
  f = str.begin();
  l = str.end();
  CHECK(p(f, l, unused));
  CHECK_EQUAL(f, l);
}

namespace {

template <class Parser>
class parser_wrapper : public parser_base<parser_wrapper<Parser>> {
public:
  parser_wrapper(int& counter, Parser x) : counter_(counter), parser_(x) {
    ++counter_;
  }

  parser_wrapper(parser_wrapper&& other)
    : counter_(other.counter_), parser_(std::move(other.parser_)) {
    ++counter_;
  }

  parser_wrapper(const parser_wrapper& other)
    : counter_(other.counter_), parser_(other.parser_) {
    ++counter_;
  }

  ~parser_wrapper() {
    --counter_;
  }

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, unused_type x) const {
    return parser_(f, l, x);
  }

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute& x) const {
    return parser_(f, l, x);
  }

private:
  int& counter_;
  Parser parser_;
};

} // namespace

TEST(recursive rule) {
  using namespace parsers;
  int num_wrappers = 0;
  { // lifetime scope of r
    rule<std::string::iterator, char> r;
    r = parser_wrapper{num_wrappers, alpha | '[' >> ref(r) >> ']'};
    auto str = "[[[x]]]"s;
    auto f = str.begin();
    auto l = str.end();
    MESSAGE("unused type");
    CHECK(r(f, l, unused));
    CHECK(f == l);
    MESSAGE("attribute");
    char c;
    f = str.begin();
    CHECK(r(f, l, c));
    CHECK(f == l);
    CHECK(c == 'x');
  }
  // Make sure no leak occured.
  CHECK_EQUAL(num_wrappers, 0);
}

// -- numeric -----------------------------------------------------------------

TEST(bool) {
  auto p0 = single_char_bool_parser{};
  auto p1 = zero_one_bool_parser{};
  auto p2 = literal_bool_parser{};
  auto str = "T0trueFfalse1"s;
  auto i = str.begin();
  auto l = str.end();
  auto f = i;
  bool b = false;

  MESSAGE("successful 'T'");
  CHECK(p0(i, l, b));
  CHECK(b);
  CHECK(i == f + 1);
  // Wrong parser
  CHECK(!p0(i, l, b));
  CHECK(i == f + 1);
  // Correct parser
  CHECK(p1(i, l, b));
  CHECK(!b);
  CHECK(i == f + 2);
  CHECK(p2(i, l, b));
  CHECK(b);
  CHECK(i == f + 6);
  // Wrong parser
  CHECK(!p2(i, l, b));
  CHECK(i == f + 6);
  // Correct parser
  CHECK(p0(i, l, b));
  CHECK(!b);
  CHECK(i == f + 7);
  b = true;
  CHECK(p2(i, l, b));
  CHECK(!b);
  CHECK(i == f + 12);
  CHECK(p1(i, l, b));
  CHECK(b);
  CHECK(i == f + 13);
  CHECK(i == l);

  MESSAGE("unused type");
  i = f;
  CHECK(p0(i, l, unused));
  CHECK(skip_to_eoi(p0)(str));
}

TEST(signed integral) {
  using namespace parsers;
  auto p = integral_parser<int>{};
  int x;
  CHECK(p("-1024", x));
  CHECK_EQUAL(x, -1024);
  CHECK(p("1024", x));
  CHECK_EQUAL(x, 1024);
  CHECK(skip_to_eoi(p)("12.34", x));
  CHECK_EQUAL(x, 12);
}

TEST(unsigned integral) {
  using namespace parsers;
  auto p = integral_parser<unsigned>{};
  unsigned x;
  CHECK(!p("-1024"));
  CHECK(p("1024", x));
  CHECK_EQUAL(x, 1024u);
  CHECK(skip_to_eoi(p)("12.34", x));
  CHECK_EQUAL(x, 12u);
}

TEST(unsigned int16) {
  using namespace parsers;
  auto p = integral_parser<uint16_t>{};
  unsigned x;
  CHECK(!p("-1024"));
  CHECK(p("1024", x));
  CHECK_EQUAL(x, 1024u);
  CHECK(p("10000", x));
  CHECK_EQUAL(x, 10000u);
  CHECK(skip_to_eoi(p)("12.34", x));
  CHECK_EQUAL(x, 12u);
}

TEST(unsigned hexadecimal integral) {
  using namespace parsers;
  auto p = ignore(-hex_prefix) >> hex64;
  unsigned x = 0u;
  CHECK(p("1234", x));
  CHECK_EQUAL(x, 0x1234u);
  CHECK(p("13BFC3d1", x));
  CHECK_EQUAL(x, 0x13BFC3d1u);
  CHECK(p("FF", x));
  CHECK_EQUAL(x, 0xFFu);
  CHECK(p("ff00", x));
  CHECK_EQUAL(x, 0xff00u);
  CHECK(p("0X12ab", x));
  CHECK_EQUAL(x, 0X12abu);
  CHECK(p("0x3e7", x));
  CHECK_EQUAL(x, 0x3e7u);
  CHECK(p("0x0000aa", x));
  CHECK_EQUAL(x, 0x0000aau);
}

TEST(signed integral with digit constraints) {
  constexpr auto max = 4;
  constexpr auto min = 2;
  auto p = integral_parser<int, max, min>{};
  int x;
  MESSAGE("not enough digits");
  CHECK(!p("1"));
  MESSAGE("within range");
  CHECK(p("12", x));
  CHECK_EQUAL(x, 12);
  CHECK(p("123", x));
  CHECK_EQUAL(x, 123);
  CHECK(p("1234", x));
  CHECK_EQUAL(x, 1234);
  MESSAGE("sign doesn't count as digit");
  CHECK(!p("-1"));
  CHECK(p("-1234", x));
  CHECK_EQUAL(x, -1234);
  MESSAGE("partial match with additional digit");
  auto str = "12345"sv;
  auto f = str.begin();
  auto l = str.end();
  CHECK(p.parse(f, l, x));
  REQUIRE(f + 1 == l);
  CHECK_EQUAL(*f, '5');
  CHECK_EQUAL(x, 1234);
  MESSAGE("partial match with non-digits character");
  str = "678x"sv;
  f = str.begin();
  l = str.end();
  CHECK(p.parse(f, l, x));
  REQUIRE(f + 1 == l);
  CHECK_EQUAL(*f, 'x');
  CHECK_EQUAL(x, 678);
}

TEST(real) {
  auto p = make_parser<double>{};
  MESSAGE("integral plus fractional part, negative");
  auto str = "-123.456789"s;
  auto f = str.begin();
  auto l = str.end();
  double d;
  CHECK(p(f, l, d));
  CHECK(d == -123.456789);
  CHECK(f == l);
  MESSAGE("integral plus fractional part, positive");
  d = 0;
  f = str.begin() + 1;
  CHECK(p(f, l, d));
  CHECK(d == 123.456789);
  CHECK(f == l);
  MESSAGE("no integral part, positive");
  d = 0;
  f = str.begin() + 4;
  CHECK(p(f, l, d));
  CHECK(d == 0.456789);
  CHECK(f == l);
  MESSAGE("no integral part, negative");
  str = "-.456789";
  f = str.begin();
  l = str.end();
  CHECK(p(f, l, d));
  CHECK(d == -0.456789);
  CHECK(f == l);
  //  MESSAGE("no fractional part, negative");
  //  d = 0;
  //  f = str.begin();
  //  CHECK(p(f, f + 4, d));
  //  CHECK(d == -123);
  //  CHECK(f == str.begin() + 4);
  //  MESSAGE("no fractional part, positive");
  //  d = 0;
  //  f = str.begin() + 1;
  //  CHECK(p(f, f + 3, d));
  //  CHECK(d == 123);
  //  CHECK(f == str.begin() + 4);
}

TEST(real - scientific) {
  auto p = make_parser<double>{};
  {
    MESSAGE("null exponent");
    auto str = ".456789e0"s;
    double d = 0;
    auto f = str.begin();
    auto l = str.end();
    CHECK(p(f, l, d));
    CHECK_EQUAL(d, 0.456789);
    CHECK_EQUAL(f, l);
  }
  {
    MESSAGE("positive exponent");
    auto str = ".456789e43"s;
    double d = 0;
    auto f = str.begin();
    auto l = str.end();
    CHECK(p(f, l, d));
    CHECK_EQUAL(d, 4.56789e42);
    CHECK_EQUAL(f, l);
  }
  {
    MESSAGE("explicit positive exponent");
    auto str = ".456789e+43"s;
    double d = 0;
    auto f = str.begin();
    auto l = str.end();
    CHECK(p(f, l, d));
    CHECK_EQUAL(d, 4.56789e42);
    CHECK_EQUAL(f, l);
  }
  {
    MESSAGE("negative exponent");
    auto str = ".456789e-322"s;
    double d = 0;
    auto f = str.begin();
    auto l = str.end();
    CHECK(p(f, l, d));
    CHECK_EQUAL(d, 4.56789e-323);
    CHECK_EQUAL(f, l);
  }
}

// This is commented out because it revealed bugs in both libstdc++ and fmt.
// Both libraries format some values incorrectly.
// TEST(real - scientific exhaustive) {
//   {
//     auto p = make_parser<real>{};
//     union real_cast {
//       real value;
//       struct comp {
//         uint64_t mantissa : 52;
//         uint16_t exponent : 11;
//         int sign : 1;
//       } components;
//     };
//     uint64_t n = 0;
//     for (uint64_t mantissa = 0; mantissa < 10000; ++mantissa) {
//       for (uint16_t exp = 0x400; exp < 0x7fe; ++exp, ++n) {
//         auto cast = real_cast{
//           .components
//           = real_cast::comp{.mantissa = mantissa, .exponent = exp, .sign =
//           0}};
//         auto value = cast.value;
//         auto rendered = fmt::format("{:e}", value);
//         auto f = rendered.begin();
//         auto l = rendered.end();
//         real d = 0;
//         if (!p(f, l, d))
//           FAIL("failed to parse " << rendered);
//         if (value != d) {
//           const auto& ic = cast.components;
//           const auto& oc = real_cast{.value = d}.components;
//           FAIL(fmt::format("[{}] parser output mismatch: {} ({}) != {}\n   "
//                            "input = {{ .mantissa = {:#015x}, .exponent = "
//                            "{:#05x}, sign = {} }}\n  output = {{ .mantissa "
//                            "= {:#015x}, .exponent = {:#05x}, sign = {} }}",
//                            n, value, rendered, d, ic.mantissa, ic.exponent,
//                            ic.sign, oc.mantissa, oc.exponent, oc.sign));
//         }
//       }
//     }
//     MESSAGE("successfully checked " << n << " generated real values");
//   }
// }

TEST(byte) {
  using namespace parsers;
  auto str = "\x01\x02\x03\x04\x05\x06\x07\x08"s;
  MESSAGE("single byte");
  auto f = str.begin();
  auto l = f + 1;
  auto u8 = uint8_t{0};
  CHECK(parsers::byte(f, l, u8));
  CHECK(u8 == 0x01u);
  CHECK(f == l);
  MESSAGE("big endian");
  f = str.begin();
  l = f + 2;
  auto u16 = uint16_t{0};
  CHECK(b16be(f, l, u16));
  CHECK(u16 == 0x0102u);
  CHECK(f == l);
  f = str.begin();
  l = f + 4;
  auto u32 = uint32_t{0};
  CHECK(b32be(f, l, u32));
  CHECK(u32 == 0x01020304ul);
  CHECK(f == l);
  f = str.begin();
  l = f + 8;
  auto u64 = uint64_t{0};
  CHECK(b64be(f, l, u64));
  CHECK(u64 == 0x0102030405060708ull);
  CHECK(f == l);
  MESSAGE("little endian");
  f = str.begin();
  l = f + 2;
  CHECK(b16le(f, l, u16));
  CHECK(u16 == 0x0201u);
  CHECK(f == l);
  f = str.begin();
  l = f + 4;
  CHECK(b32le(f, l, u32));
  CHECK(u32 == 0x04030201ul);
  CHECK(f == l);
  f = str.begin();
  l = f + 8;
  CHECK(b64le(f, l, u64));
  CHECK(u64 == 0x0807060504030201ull);
  CHECK(f == l);
  MESSAGE("variable length");
  f = str.begin();
  l = f + 3;
  std::array<uint8_t, 3> a3;
  a3.fill(0);
  CHECK(bytes<3>(f, l, a3));
  CHECK(a3[0] == 0x01);
  CHECK(a3[1] == 0x02);
  CHECK(a3[2] == 0x03);
  f = str.begin();
  l = f + 5;
  std::array<uint8_t, 5> a5;
  a5.fill(0);
  CHECK(bytes<5>(f, l, a5));
  CHECK(a5[0] == 0x01);
  CHECK(a5[1] == 0x02);
  CHECK(a5[2] == 0x03);
  CHECK(a5[3] == 0x04);
  CHECK(a5[4] == 0x05);
  std::array<uint8_t, 8> a8;
  CHECK(bytes<8>(str, a8));
  CHECK(a8[0] == 0x01);
  CHECK(a8[1] == 0x02);
  CHECK(a8[2] == 0x03);
  CHECK(a8[3] == 0x04);
  CHECK(a8[4] == 0x05);
  CHECK(a8[5] == 0x06);
  CHECK(a8[6] == 0x07);
  CHECK(a8[7] == 0x08);
  auto ip = "\xdf\x00\x0d\xb8\x00\x00\x00\x00\x02\x02\xb3\xff\xfe\x1e\x83\x28"s;
  std::array<uint8_t, 16> a16;
  CHECK(bytes<16>(ip, a16));
  CHECK(a16[0] == 0xdf);
  CHECK(a16[1] == 0x00);
  CHECK(a16[2] == 0x0d);
  CHECK(a16[3] == 0xb8);
  CHECK(a16[4] == 0x00);
  CHECK(a16[5] == 0x00);
  CHECK(a16[6] == 0x00);
  CHECK(a16[7] == 0x00);
  CHECK(a16[8] == 0x02);
  CHECK(a16[9] == 0x02);
  CHECK(a16[10] == 0xb3);
  CHECK(a16[11] == 0xff);
  CHECK(a16[12] == 0xfe);
  CHECK(a16[13] == 0x1e);
  CHECK(a16[14] == 0x83);
  CHECK(a16[15] == 0x28);
}

TEST(byte - type promotion regression) {
  using namespace parsers;
  uint16_t x;
  CHECK(b16be("\x00\x8d"s, x));
  CHECK_EQUAL(x, 0x8du);
  CHECK(b16le("\x8d\x00"s, x));
  CHECK_EQUAL(x, 0x8du);
  uint32_t y;
  CHECK(b32be("\x00\x00\x00\x8d"s, y));
  CHECK_EQUAL(y, 0x8dul);
  CHECK(b32le("\x8d\x00\x00\x00"s, y));
  CHECK_EQUAL(y, 0x8dul);
  uint64_t z;
  CHECK(b64be("\x00\x00\x00\x00\x00\x00\x00\x8d"s, z));
  CHECK_EQUAL(z, 0x8dull);
  CHECK(b64le("\x8d\x00\x00\x00\x00\x00\x00\x00"s, z));
  CHECK_EQUAL(z, 0x8dull);
}

TEST(dynamic bytes) {
  using namespace parsers;
  std::string foo;
  auto three = 3;
  CHECK(skip_to_eoi(nbytes<char>(three))("foobar"s, foo));
  CHECK_EQUAL(foo, "foo"s);
  MESSAGE("input too short");
  foo.clear();
  auto two = 2;
  CHECK(skip_to_eoi(nbytes<char>(two))("foobar"s, foo));
  CHECK_EQUAL(foo, "fo"s);
  MESSAGE("input too large");
  foo.clear();
  auto seven = 7;
  CHECK(!skip_to_eoi(nbytes<char>(seven))("foobar"s, foo));
  CHECK_EQUAL(foo, "foobar"s);
}

// -- time --------------------------------------------------------------------

TEST(time - now) {
  tenzir::time ts;
  CHECK(parsers::time("now", ts));
  CHECK(ts > time::min()); // must be greater than the UNIX epoch
}

TEST(time - YMD) {
  using namespace std::chrono;
  tenzir::time ts;
  CHECK(parsers::time("2017-08-13", ts));
  auto utc_secs = seconds{1502582400};
  CHECK_EQUAL(ts.time_since_epoch(), utc_secs);
  CHECK(parsers::time("2017-08-13+21:10:42", ts));
  utc_secs = std::chrono::seconds{1502658642};
  CHECK_EQUAL(ts.time_since_epoch(), utc_secs);
}

// -- SI literals -------------------------------------------------------------

namespace {

template <class T>
T to_si(std::string_view str) {
  auto parse_si = [&](auto input, auto& x) {
    if constexpr (std::is_same_v<T, uint64_t>)
      return parsers::count(input, x);
    else if constexpr (std::is_same_v<T, int64_t>)
      return parsers::integer(input, x);
  };
  T x;
  if (!parse_si(str, x))
    FAIL("could not parse " << str << " as SI literal");
  return x;
}

} // namespace

TEST(si count) {
  auto to_count = to_si<uint64_t>;
  using namespace si_literals;
  CHECK_EQUAL(to_count("42"), 42u);
  CHECK_EQUAL(to_count("1k"), 1_k);
  CHECK_EQUAL(to_count("2M"), 2_M);
  CHECK_EQUAL(to_count("3G"), 3_G);
  CHECK_EQUAL(to_count("4T"), 4_T);
  CHECK_EQUAL(to_count("5E"), 5_E);
  CHECK_EQUAL(to_count("6Ki"), 6_Ki);
  CHECK_EQUAL(to_count("7Mi"), 7_Mi);
  CHECK_EQUAL(to_count("8Gi"), 8_Gi);
  CHECK_EQUAL(to_count("9Ti"), 9_Ti);
  CHECK_EQUAL(to_count("10Ei"), 10_Ei);
  MESSAGE("spaces before unit");
  CHECK_EQUAL(to_count("1 Mi"), 1_Mi);
  CHECK_EQUAL(to_count("1  Mi"), 1_Mi);
}

TEST(si int) {
  auto to_int = to_si<int64_t>;
  auto as_int = [](auto x) {
    return int64_t{detail::narrow_cast<int64_t>(x)};
  };
  using namespace si_literals;
  CHECK_EQUAL(to_int("-42"), -as_int(42));
  CHECK_EQUAL(to_int("-1k"), -as_int(1_k));
  CHECK_EQUAL(to_int("-2M"), -as_int(2_M));
  CHECK_EQUAL(to_int("-3G"), -as_int(3_G));
  CHECK_EQUAL(to_int("-4T"), -as_int(4_T));
  CHECK_EQUAL(to_int("-5E"), -as_int(5_E));
  CHECK_EQUAL(to_int("-6Ki"), -as_int(6_Ki));
  CHECK_EQUAL(to_int("-7Mi"), -as_int(7_Mi));
  CHECK_EQUAL(to_int("-8Gi"), -as_int(8_Gi));
  CHECK_EQUAL(to_int("-9Ti"), -as_int(9_Ti));
  CHECK_EQUAL(to_int("-10Ei"), -as_int(10_Ei));
}

TEST(bytesize) {
  const auto parse = [](std::string_view str) {
    if (auto result = uint64_t{}; parsers::bytesize(str, result))
      return result;
    FAIL("failed to parse bytesize: " << str);
  };
  using namespace si_literals;
  CHECK_EQUAL(parse("42"), 42u);
  CHECK_EQUAL(parse("1k"), 1_k);
  CHECK_EQUAL(parse("2M"), 2_M);
  CHECK_EQUAL(parse("3G"), 3_G);
  CHECK_EQUAL(parse("4T"), 4_T);
  CHECK_EQUAL(parse("5E"), 5_E);
  CHECK_EQUAL(parse("6Ki"), 6_Ki);
  CHECK_EQUAL(parse("7Mi"), 7_Mi);
  CHECK_EQUAL(parse("8Gi"), 8_Gi);
  CHECK_EQUAL(parse("9Ti"), 9_Ti);
  CHECK_EQUAL(parse("10Ei"), 10_Ei);
  CHECK_EQUAL(parse("1 Mi"), 1_Mi);
  CHECK_EQUAL(parse("1  Mi"), 1_Mi);
  CHECK_EQUAL(parse("42B"), 42u);
  CHECK_EQUAL(parse("1kB"), 1_k);
  CHECK_EQUAL(parse("2MB"), 2_M);
  CHECK_EQUAL(parse("3GB"), 3_G);
  CHECK_EQUAL(parse("4TB"), 4_T);
  CHECK_EQUAL(parse("5EB"), 5_E);
  CHECK_EQUAL(parse("6KiB"), 6_Ki);
  CHECK_EQUAL(parse("7MiB"), 7_Mi);
  CHECK_EQUAL(parse("8GiB"), 8_Gi);
  CHECK_EQUAL(parse("9TiB"), 9_Ti);
  CHECK_EQUAL(parse("10EiB"), 10_Ei);
  CHECK_EQUAL(parse("1 MiB"), 1_Mi);
  CHECK_EQUAL(parse("1  MiB"), 1_Mi);
}

// -- option set --------------------------------------------------------------

TEST(option set - no options defined) {
  const auto options = option_set_parser{{}};
  auto pipeline_options = R"(--option="o" --invalid="i" field1)";
  auto pipeline_options_view = std::string_view{pipeline_options};
  const auto* f = pipeline_options_view.begin();
  const auto* const l = pipeline_options_view.end();
  auto parsed_options = std::unordered_map<std::string, data>{};
  auto success = options(f, l, parsed_options);
  REQUIRE(success);
  REQUIRE(parsed_options.empty());
  REQUIRE_EQUAL(f, pipeline_options_view.begin());
}

TEST(option set - long form options) {
  const auto options = option_set_parser{{{"option", 'o'}, {"valid", 'v'}}};
  auto pipeline_options = R"(--option = "value" --valid=12345 field1)";
  auto pipeline_options_view = std::string_view{pipeline_options};
  const auto* f = pipeline_options_view.begin();
  const auto* const l = pipeline_options_view.end();
  auto parsed_options = std::unordered_map<std::string, data>{};
  auto success = options(f, l, parsed_options);
  REQUIRE(success);
  REQUIRE_EQUAL(parsed_options.size(), size_t{2});
  REQUIRE_NOT_EQUAL(f, pipeline_options_view.begin());
  REQUIRE_EQUAL((*caf::get_if<std::string>(&parsed_options.at("option"))),
                "value");
  REQUIRE_EQUAL((*caf::get_if<uint64_t>(&parsed_options.at("valid"))), 12345u);
}

TEST(option set - short form options) {
  const auto options = option_set_parser{{{"option", 'o'}, {"valid", 'v'}}};
  auto pipeline_options = R"(-o "value" -v 12345 field1)";
  auto pipeline_options_view = std::string_view{pipeline_options};
  const auto* f = pipeline_options_view.begin();
  const auto* const l = pipeline_options_view.end();
  auto parsed_options = std::unordered_map<std::string, data>{};
  auto success = options(f, l, parsed_options);
  REQUIRE(success);
  REQUIRE_EQUAL(parsed_options.size(), size_t{2});
  REQUIRE_NOT_EQUAL(f, pipeline_options_view.begin());
  REQUIRE_EQUAL((*caf::get_if<std::string>(&parsed_options.at("option"))),
                "value");
  REQUIRE_EQUAL((*caf::get_if<uint64_t>(&parsed_options.at("valid"))), 12345u);
}

TEST(option set - long form options mixed with short form options) {
  const auto options
    = option_set_parser{{{"option", 'o'}, {"valid", 'v'}, {"short", 's'}}};
  auto pipeline_options = R"(-o "value" --valid=12345 -s 2 field1)";
  auto pipeline_options_view = std::string_view{pipeline_options};
  const auto* f = pipeline_options_view.begin();
  const auto* const l = pipeline_options_view.end();
  auto parsed_options = std::unordered_map<std::string, data>{};
  auto success = options(f, l, parsed_options);
  REQUIRE(success);
  REQUIRE_EQUAL(parsed_options.size(), size_t{3});
  REQUIRE_NOT_EQUAL(f, pipeline_options_view.begin());
  REQUIRE_EQUAL((*caf::get_if<std::string>(&parsed_options.at("option"))),
                "value");
  REQUIRE_EQUAL((*caf::get_if<uint64_t>(&parsed_options.at("valid"))), 12345u);
  REQUIRE_EQUAL((*caf::get_if<uint64_t>(&parsed_options.at("short"))), 2u);
}

TEST(option set - invalid long form option syntax) {
  const auto options = option_set_parser{{{"option", 'o'}, {"valid", 'v'}}};
  auto pipeline_options = R"(--option "value" --valid=12345 field1)";
  auto pipeline_options_view = std::string_view{pipeline_options};
  const auto* f = pipeline_options_view.begin();
  const auto* const l = pipeline_options_view.end();
  auto parsed_options = std::unordered_map<std::string, data>{};
  auto success = options(f, l, parsed_options);
  REQUIRE(!success);
  REQUIRE(parsed_options.empty());
  REQUIRE_EQUAL(f, pipeline_options_view.begin());
}

TEST(option set - invalid short form option syntax) {
  const auto options = option_set_parser{{{"option", 'o'}, {"valid", 'v'}}};
  auto pipeline_options = R"(-o="value" -v 12345 field1)";
  auto pipeline_options_view = std::string_view{pipeline_options};
  const auto* f = pipeline_options_view.begin();
  const auto* const l = pipeline_options_view.end();
  auto parsed_options = std::unordered_map<std::string, data>{};
  auto success = options(f, l, parsed_options);
  REQUIRE(!success);
  REQUIRE(parsed_options.empty());
  REQUIRE_EQUAL(f, pipeline_options_view.begin());
}

TEST(option set - option value defined twice gets overwritten) {
  const auto options = option_set_parser{{{"option", 'o'}, {"valid", 'v'}}};
  auto pipeline_options = R"(--option = "value" -o "value2" field1)";
  auto pipeline_options_view = std::string_view{pipeline_options};
  const auto* f = pipeline_options_view.begin();
  const auto* const l = pipeline_options_view.end();
  auto parsed_options = std::unordered_map<std::string, data>{};
  auto success = options(f, l, parsed_options);
  REQUIRE(success);
  REQUIRE_EQUAL(parsed_options.size(), size_t{1});
  REQUIRE_NOT_EQUAL(f, pipeline_options_view.begin());
  REQUIRE_EQUAL((*caf::get_if<std::string>(&parsed_options.at("option"))),
                "value2");
}

TEST(option set - missing option value) {
  const auto options = option_set_parser{{{"option", 'o'}}};
  auto pipeline_options = R"(--option =)";
  auto pipeline_options_view = std::string_view{pipeline_options};
  const auto* f = pipeline_options_view.begin();
  const auto* const l = pipeline_options_view.end();
  auto parsed_options = std::unordered_map<std::string, data>{};
  auto success = options(f, l, parsed_options);
  REQUIRE(!success);
  REQUIRE(parsed_options.empty());
  REQUIRE_EQUAL(f, pipeline_options_view.begin());
}

// -- API ---------------------------------------------------------------------

TEST(range) {
  const auto s = "1,2,3"sv;
  offset xs;
  auto begin = s.begin();
  auto end = s.end();
  CHECK(parse(begin, end, xs));
  CHECK(begin == end);
  CHECK_EQUAL(xs, (offset{1, 2, 3}));
}

TEST(to) {
  auto xs = to<offset>("1,2,3");
  REQUIRE(xs);
  CHECK_EQUAL(*xs, (offset{1, 2, 3}));
}

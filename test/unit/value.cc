#include "framework/unit.h"
#include <sstream>
#include "vast/value.h"

SUITE("value")

using namespace vast;

TEST("size")
{
  CHECK(sizeof(value) <= 32);
}

namespace {
template <type_tag T, typename U>
bool constexpr type_tag_check()
{
  return std::is_same<type_tag_type<T>, U>::value;
}
} // namespace anonymous

TEST("type tags")
{
  CHECK((type_tag_check<invalid_value, value_invalid>()));
  CHECK((type_tag_check<bool_value, bool>()));
  CHECK((type_tag_check<int_value, int64_t>()));
  CHECK((! type_tag_check<int_value, int>()));
  CHECK((type_tag_check<uint_value, uint64_t>()));
  CHECK((type_tag_check<double_value, double>()));
  CHECK((type_tag_check<string_value, string>()));
}

TEST("trivial")
{
  value v1;
  CHECK(! v1.nil());
  CHECK(v1.invalid());
  CHECK(v1.which() == invalid_value);
  CHECK(to_string(v1) == "<invalid>");

  value v2{true};
  CHECK(v2.which() == bool_value);
  v2.clear();
  CHECK(! v2);
  CHECK(v2.nil());
  CHECK(! v2.invalid());
  v2 = "foo";
  CHECK(v2.which() == string_value);
  v2.clear();

  value v3(v2);
  CHECK(v3.nil());
  CHECK(! v3.invalid());
  CHECK(v3.which() == string_value);

  value v4(string_value);
  CHECK(! v4);
  CHECK(! v4.invalid());
  CHECK(v4.nil());
}

TEST("relational operators")
{
  value v1, v2;
  CHECK(v1 == v2);
  CHECK(! (v1 != v2));

  v1 = "foo";
  CHECK(v1.which() == string_value);
  CHECK(v1 != v2);
  CHECK(! (v1 == v2));
  CHECK(! (v1 < v2));
  CHECK(! (v1 > v2));
  CHECK(! (v1 <= v2));
  CHECK(! (v1 >= v2));

  v2 = 42;
  CHECK(v2.which() == int_value);
  CHECK(v1 != v2);
  CHECK(! (v1 == v2));
  CHECK(! (v1 < v2));
  CHECK(! (v1 > v2));
  CHECK(! (v1 <= v2));
  CHECK(! (v1 >= v2));

  v2 = "foo";
  CHECK(v2.which() == string_value);
  CHECK(v1 == v2);
  CHECK(! (v1 != v2));
  CHECK(! (v1 < v2));
  CHECK(! (v1 > v2));
  CHECK(v1 <= v2);
  CHECK(v1 >= v2);
}

TEST("bool")
{
  value v1(true);
  value v2(false);
  CHECK(to_string(v1) == "T");
  CHECK(to_string(v2) == "F");
  v1 = false;
  v2 = true;
  CHECK(to_string(v1) == "F");
  CHECK(to_string(v2) == "T");
  CHECK(v1 != v2);
  CHECK(v1 < v2);
  CHECK(v1 == false);
  CHECK(v2 == true);

  value v3(bool_value);
  CHECK(to_string(v3) == "<bool>");
  value v4(double_value);
  CHECK(to_string(v4) == "<double>");
}

TEST("integer")
{
  value v1(42);
  value v2(42u);
  CHECK(v1.which() == int_value);
  CHECK(v2.which() == uint_value);
  CHECK(to_string(v1) == "+42");
  CHECK(to_string(v2) == "42");
  CHECK(v1 != v2);  // Not comparable due to different signedness.
  v1 = -1;
  v2 = 0;
  CHECK(to_string(v1) == "-1");
  CHECK(to_string(v2) == "+0");
  v2 = -99999999;
  CHECK(v1 > v2);
  CHECK(v1 != v2);
}

TEST("floating point")
{
  value v1(0.0);
  CHECK(v1.which() == double_value);
  CHECK(to_string(v1) == "0.0000000000");
  CHECK(v1 == 0.0);

  value v2 = 0.123456789;
  CHECK(to_string(v2) == "0.1234567890");
  CHECK(v2 < 123.456789);
  CHECK(v2.get<double>() == 0.123456789);

  v2 = -123.456;
  CHECK(to_string(v2) == "-123.4560000000");
}

TEST("string")
{
  value empty("");
  CHECK(empty.get<string>().size() == 0);
  CHECK(std::strcmp(empty.get<string>().data(), "") == 0);

  value v1('c');
  CHECK(v1.which() == string_value);
  CHECK(to_string(v1) == "\"c\"");
  v1 = 'x';
  CHECK(to_string(v1) == "\"x\"");

  value v2("foo");
  CHECK(v2 == "foo");
  CHECK(to_string(v2) == "\"foo\"");
  CHECK(v2.get<string>().size() == 3);
  v2 = "quux";
  CHECK(v2 == "quux");
  CHECK(to_string(v2) == "\"quux\"");
  CHECK(v2.get<string>().size() == 4);
  CHECK(to_string(v2) == "\"quux\"");

  // Testing the limits.
  std::string str(string::in_situ_size, 'x');
  value v3(str.data());
  CHECK(! v3.get<string>().is_heap_allocated());
  CHECK(v3 == str.data());
  str.push_back('y');
  v3 = str.data();   // Creates a copy on the heap...
  CHECK(v3 == str.data());
  CHECK(v3.get<string>().is_heap_allocated());
  str.pop_back();
  v3 = str.data();   // ...and is placed back into the in-situ buffer.
  CHECK(v3 == str.data());
  CHECK(! v3.get<string>().is_heap_allocated());

  auto phrase = "Das ist also des Pudels Kern.";
  value v4;
  v4 = phrase;
  CHECK(v4 == phrase);
  CHECK(! v4.get<string>().is_heap_allocated());
  CHECK(v4.get<string>().size() == std::strlen(phrase));

  auto nul = "ro\0ot";
  value v5(nul, 5);
  auto const& s = v5.get<string>();
  CHECK(std::string(nul, 5) == std::string(s.begin(), s.end()));

  CHECK(v4 == phrase);
  CHECK(v5 == value("ro\0ot", 5));
  CHECK(v4 < v5);
}

TEST("regexes")
{
  regex r{"."};
  value v1(r);
  CHECK(v1.which() == regex_value);
  CHECK(v1.get<regex>() == r);
  CHECK(to_string(v1) == "/./");
}

TEST("time_range & time_point")
{
  auto jetzt = now();
  value t(jetzt);
  value d(jetzt.since_epoch());

  CHECK(jetzt == t.get<time_point>());
  CHECK(jetzt.since_epoch() == d.get<time_range>());

  value r(std::chrono::seconds(72));
  CHECK(r == time_range::seconds(72));

  // Testing template instantiation of operator=.
  r = std::chrono::system_clock::now();
}

TEST("containers")
{
  record r{"foo", 42u, -4711, *address::from_v6("dead::beef")};
  value vr(std::move(r));
  CHECK(to_string(vr), "(\"foo\", 42, -4711 == dead::beef)");
  vr.get<record>().emplace_back("qux");
  vr.get<record>().emplace_back("corge");
  CHECK(vr.get<record>().size() == 6);

  table t{{-1, 10u}, {-2, 20u}, {-3, 30u}};
  value vt(t);
  CHECK(to_string(vt), "{-3 -> 30, -2 -> 20 == -1 -> 10}");
  auto& tbl = vt.get<table>();
  CHECK(t == tbl);
  tbl[-1] = 15u;
  tbl[0] = 42u;
  CHECK(tbl.begin()->second == 30u);
  CHECK(tbl[0] == 42u);
  CHECK(tbl[-1] == 15u);
  CHECK(to_string(vt), "{-3 -> 30, -2 -> 20, -1 -> 15 == +0 -> 42}");
}

TEST("address")
{
  value v1(*address::from_v4("10.1.1.2"));
  CHECK(v1.which() == address_value);
  CHECK(to_string(v1) == "10.1.1.2");

  v1 = *address::from_v4("127.0.0.1");
  CHECK(v1.get<address>().is_loopback());
  CHECK(to_string(v1) == "127.0.0.1");

  value v2(*address::from_v6("f00::babe"));
  CHECK(v2.get<address>().is_v6());
  CHECK(v1 != v2);
}

TEST("prefix")
{
  value v1(prefix{*address::from_v4("10.1.1.2"), 8});
  CHECK(v1.which() == prefix_value);
  CHECK(to_string(v1) == "10.0.0.0/8");
  CHECK(v1.get<prefix>().length() == 8);

  v1 = prefix{*address::from_v4("127.0.0.1"), 32};
  CHECK(to_string(v1) == "127.0.0.1/32");
  CHECK(v1.get<prefix>().length() == 32);
}

TEST("port")
{
  value v1(port{8, port::icmp});
  CHECK(v1.which() == port_value);
  CHECK(to_string(v1) == "8/icmp");
  v1 = port{25, port::tcp};
  CHECK(to_string(v1) == "25/tcp");
  CHECK(v1.get<port>().number() == 25);
}

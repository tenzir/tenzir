#include "test.h"
#include "vast/type.h"
#include "vast/value.h"
#include "vast/util/convert.h"

using namespace vast;

BOOST_AUTO_TEST_CASE(type_creation_and_display)
{
  BOOST_CHECK_EQUAL(to_string(*type::make<invalid_type>()), "<invalid>");
  BOOST_CHECK_EQUAL(to_string(*type::make<bool_type>()), "bool");
  BOOST_CHECK_EQUAL(to_string(*type::make<int_type>()), "int");
  BOOST_CHECK_EQUAL(to_string(*type::make<uint_type>()), "count");
  BOOST_CHECK_EQUAL(to_string(*type::make<double_type>()), "double");
  BOOST_CHECK_EQUAL(to_string(*type::make<time_range_type>()), "interval");
  BOOST_CHECK_EQUAL(to_string(*type::make<time_point_type>()), "time");
  BOOST_CHECK_EQUAL(to_string(*type::make<string_type>()), "string");
  BOOST_CHECK_EQUAL(to_string(*type::make<regex_type>()), "pattern");
  BOOST_CHECK_EQUAL(to_string(*type::make<address_type>()), "addr");
  BOOST_CHECK_EQUAL(to_string(*type::make<prefix_type>()), "prefix");
  BOOST_CHECK_EQUAL(to_string(*type::make<port_type>()), "port");

  std::vector<string> f;
  f.emplace_back("foo");
  f.emplace_back("bar");
  BOOST_CHECK_EQUAL(to_string(*type::make<enum_type>(f)), "enum {foo, bar}");

  auto b = type::make<bool_type>();
  BOOST_CHECK_EQUAL(to_string(*b), "bool");

  auto s = type::make<set_type>(b);
  BOOST_CHECK_EQUAL(to_string(*s), "set[bool]");

  auto v = type::make<vector_type>(b);
  BOOST_CHECK_EQUAL(to_string(*v), "vector of bool");

  auto t = type::make<table_type>(b, s);
  BOOST_CHECK_EQUAL(to_string(*t), "table[bool] of set[bool]");

  std::vector<argument> args;
  args.emplace_back("foo", b);
  args.emplace_back("bar", s);
  auto r = type::make<record_type>(args);
  BOOST_CHECK_EQUAL(to_string(*r), "record {foo: bool, bar: set[bool]}");

  event_info e{"qux", args};
  BOOST_CHECK_EQUAL(to_string(e), "qux(foo: bool, bar: set[bool])");

  // Name a type.
  s->name("bool_set");
  BOOST_CHECK_EQUAL(to_string(*s), "bool_set");
  BOOST_CHECK_EQUAL(to_string(*t), "table[bool] of bool_set");
}

BOOST_AUTO_TEST_CASE(type_construction)
{
  auto b = type::make<bool_type>();
  BOOST_CHECK_EQUAL(b->tag(), bool_value);

  auto e = type::make<enum_type>();
  BOOST_CHECK_EQUAL(e->tag(), invalid_value);
}

BOOST_AUTO_TEST_CASE(type_mapping)
{
  BOOST_CHECK_EQUAL(to_value_type<invalid_type>::value, invalid_value);
  BOOST_CHECK_EQUAL(to_value_type<bool_type>::value, bool_value);
  BOOST_CHECK_EQUAL(to_value_type<int_type>::value, int_value);
  BOOST_CHECK_EQUAL(to_value_type<uint_type>::value, uint_value);
  // ...
  BOOST_CHECK_EQUAL(to_value_type<record_type>::value, record_value);
}

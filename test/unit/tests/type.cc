#include "framework/unit.h"

#include "vast/value.h"
#include "vast/util/convert.h"

SUITE("type system")

using namespace vast;

TEST("construction and display")
{
  CHECK(to_string(*type::make<invalid_type>()) == "<invalid>");
  CHECK(to_string(*type::make<bool_type>()) == "bool");
  CHECK(to_string(*type::make<int_type>()) == "int");
  CHECK(to_string(*type::make<uint_type>()) == "count");
  CHECK(to_string(*type::make<double_type>()) == "double");
  CHECK(to_string(*type::make<time_range_type>()) == "interval");
  CHECK(to_string(*type::make<time_point_type>()) == "time");
  CHECK(to_string(*type::make<string_type>()) == "string");
  CHECK(to_string(*type::make<regex_type>()) == "pattern");
  CHECK(to_string(*type::make<address_type>()) == "addr");
  CHECK(to_string(*type::make<prefix_type>()) == "subnet");
  CHECK(to_string(*type::make<port_type>()) == "port");

  std::vector<string> f;
  f.emplace_back("foo");
  f.emplace_back("bar");
  CHECK(to_string(*type::make<enum_type>("", f)) == "enum {foo, bar}");

  auto b = type::make<bool_type>();
  CHECK(to_string(*b) == "bool");

  auto s = type::make<set_type>("", b);
  CHECK(to_string(*s) == "set[bool]");

  auto v = type::make<vector_type>("", b);
  CHECK(to_string(*v) == "vector[bool]");

  auto t = type::make<table_type>("", b, s);
  CHECK(to_string(*t) == "table[bool] of set[bool]");

  std::vector<argument> args;
  args.emplace_back("foo", b);
  args.emplace_back("bar", s);
  auto r = type::make<record_type>("", args);
  CHECK(to_string(*r) == "record {foo: bool, bar: set[bool]}");

  // Clone a type to give it a new name.
  s = s->clone("bool_set");
  CHECK(to_string(*s) == "bool_set");
  CHECK(to_string(*t) == "table[bool] of set[bool]");
}

TEST("construction")
{
  auto b = type::make<bool_type>();
  CHECK(b->tag() == bool_value);

  auto e = type::make<enum_type>();
  CHECK(e->tag() == invalid_value);
}

TEST("mappings")
{
  CHECK(to_type_tag<invalid_type>::value == invalid_value);
  CHECK(to_type_tag<bool_type>::value == bool_value);
  CHECK(to_type_tag<int_type>::value == int_value);
  CHECK(to_type_tag<uint_type>::value == uint_value);
  // ...
  CHECK(to_type_tag<record_type>::value == record_value);
}

TEST("record resolving")
{
  std::vector<argument> args0;
  args0.emplace_back("x", type::make<int_type>());
  args0.emplace_back("y", type::make<address_type>());
  args0.emplace_back("z", type::make<double_type>());

  std::vector<argument> args1;
  args1.emplace_back("a", type::make<int_type>());
  args1.emplace_back("b", type::make<uint_type>());
  args1.emplace_back("c", type::make<record_type>("", std::move(args0)));

  record_type rt{std::move(args1)};

  auto o = rt.resolve({"c"});
  REQUIRE(o);
  CHECK(o->size() == 1);
  CHECK(o->front() == 2);

  o = rt.resolve({"c", "x"});
  REQUIRE(o);
  CHECK(o->size() == 2);
  CHECK(o->front() == 2);
  CHECK(o->back() == 0);
}

TEST("symbol finding")
{
  std::vector<argument> args0;
  args0.emplace_back("x", type::make<int_type>());
  args0.emplace_back("y", type::make<address_type>());
  args0.emplace_back("z", type::make<double_type>());

  std::vector<argument> args1;
  args1.emplace_back("a", type::make<int_type>());
  args1.emplace_back("b", type::make<uint_type>());
  args1.emplace_back("c", type::make<record_type>("", std::move(args0)));

  std::vector<argument> args2;
  args2.emplace_back("a", type::make<int_type>());
  args2.emplace_back("b", type::make<record_type>("", std::move(args1)));
  args2.emplace_back("c", type::make<uint_type>());

  record_type rt{std::move(args2)};

  // Record access by key.
  auto first = rt.at(key{"a"});
  REQUIRE(first);
  CHECK(first->tag() == int_value);

  auto deep = rt.at(key{"b", "c", "y"});
  REQUIRE(deep);
  CHECK(deep->tag() == address_value);

  // Prefix finding.
  auto o = rt.find_prefix({"a"});
  offset a{0};
  REQUIRE(o.size() == 1);
  CHECK(o[0].first == a);

  o = rt.find_prefix({"b", "a"});
  offset ba{1, 0};
  REQUIRE(o.size() == 1);
  CHECK(o[0].first == ba);

  // Suffix finding.
  o = rt.find_suffix({"z"});
  offset z{1, 2, 2};
  REQUIRE(o.size() == 1);
  CHECK(o[0].first == z);

  o = rt.find_suffix({"c", "y"});
  offset cy{1, 2, 1};
  REQUIRE(o.size() == 1);
  CHECK(o[0].first == cy);

  o = rt.find_suffix({"a"});
  offset a0{0}, a1{1, 0};
  REQUIRE(o.size() == 2);
  CHECK(o[0].first == a0);
  CHECK(o[1].first == a1);

  o = rt.find_suffix({"c", "*"});
  offset c0{1, 2, 0}, c1{1, 2, 1}, c2{1, 2, 2};
  REQUIRE(o.size() == 3);
  CHECK(o[0].first == c0);
  CHECK(o[1].first == c1);
  CHECK(o[2].first == c2);

  //
  // Types
  //

  auto t = type::make<record_type>("foo", std::move(rt));

  // Type access by key.
  first = t->at(key{"foo", "a"});
  REQUIRE(first);
  CHECK(first->tag() == int_value);

  auto deep_key = key{"foo", "b", "c", "y"};
  deep = t->at(deep_key);
  REQUIRE(deep);
  CHECK(deep->tag() == address_value);

  auto cast = t->cast(deep_key);
  REQUIRE(cast);
  CHECK(*cast == cy);

  // Prefix finding.
  o = t->find_prefix({"foo", "a"});
  REQUIRE(o.size() == 1);
  CHECK(o[0].first == a0);

  // Matches everything.
  o = t->find_prefix({"foo", "*"});
  REQUIRE(o.size() == 7);

  // Same as finding the suffix for "a".
  o = t->find_suffix({"*", "a"});
  REQUIRE(o.size() == 2);
  CHECK(o[0].first == a0);
  CHECK(o[1].first == a1);

  //for (auto& p : o)
  //  std::cout << p.first << "\t\t" << p.second << std::endl;
}

TEST("compatibility")
{
  auto i = type::make<int_type>();
  auto u = type::make<uint_type>();

  CHECK(i->represents(i));
  CHECK(! i->represents(u));

  auto s = type::make<set_type>("jiggle", i);
  CHECK(s->represents(type::make<set_type>("foozen", i)));
  CHECK(! s->represents(type::make<set_type>("barzen", u)));

  std::vector<argument> args0;
  args0.emplace_back("x", type::make<int_type>());
  args0.emplace_back("y", type::make<address_type>());
  args0.emplace_back("z", type::make<double_type>());

  std::vector<argument> args1;
  args1.emplace_back("a", type::make<int_type>());
  args1.emplace_back("b", type::make<address_type>());
  args1.emplace_back("c", type::make<double_type>());

  auto t0 = type::make<record_type>("foo", std::move(args0));
  auto t1 = type::make<record_type>("bar", std::move(args1));

  CHECK(t0->represents(t1));
  CHECK(t1->represents(t0));
}

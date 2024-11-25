//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/data.hpp"

#include "tenzir/concept/convertible/to.hpp"
#include "tenzir/concept/parseable/tenzir/data.hpp"
#include "tenzir/concept/parseable/to.hpp"
#include "tenzir/concept/printable/stream.hpp"
#include "tenzir/concept/printable/tenzir/data.hpp"
#include "tenzir/concept/printable/tenzir/json.hpp"
#include "tenzir/concept/printable/to_string.hpp"
#include "tenzir/detail/legacy_deserialize.hpp"
#include "tenzir/detail/serialize.hpp"
#include "tenzir/fbs/data.hpp"
#include "tenzir/flatbuffer.hpp"
#include "tenzir/operator.hpp"
#include "tenzir/test/test.hpp"

#include <caf/test/dsl.hpp>

using namespace tenzir;
using namespace std::chrono_literals;
using namespace std::string_literals;

TEST(list) {
  REQUIRE((std::is_same_v<std::vector<data>, list>));
}

TEST(maps) {
  map ports{{"ssh", 22u}, {"http", 80u}, {"https", 443u}, {"imaps", 993u}};
  CHECK(ports.size() == 4);
  auto i = ports.find("ssh");
  REQUIRE(i != ports.end());
  CHECK(i->second == 22u);
  i = ports.find("imaps");
  REQUIRE(i != ports.end());
  CHECK(i->second == 993u);
  CHECK(ports.emplace("telnet", 23u).second);
  CHECK(!ports.emplace("http", 8080u).second);
}

TEST(merge) {
  // clang-format off
  const auto xs = record{
    {"a", "foo"},
    {"b", record{
      {"c", int64_t{-42}},
      {"d", list{int64_t{1}, int64_t{2}, int64_t{3}}}
    }},
    {"c", record{
      {"a", "bar"}
    }}
  };
  const auto ys = record{
    {"a", "bar"},
    {"b", record{
      {"a", int64_t{42}},
      {"d", list{int64_t{4}, int64_t{5}, int64_t{6}}}
    }},
    {"c", "not a record yet"}
  };
  {
    auto expected = record{
      {"a", "foo"},
      {"b", record{
        {"a", int64_t{42}},
        {"d", list{int64_t{1}, int64_t{2}, int64_t{3}}},
        {"c", int64_t{-42}}
      }},
      {"c", record{
        {"a", "bar"}
      }}
    };
    auto copy = ys;
    merge(xs, copy, policy::merge_lists::no);
    CHECK_EQUAL(copy, expected);
  }
  {
    auto expected = record{
      {"a", "foo"},
      {"b", record{
        {"a", int64_t{42}},
        {"d", list{int64_t{4}, int64_t{5}, int64_t{6},
                   int64_t{1}, int64_t{2}, int64_t{3}}},
        {"c", int64_t{-42}}
      }},
      {"c", record{
        {"a", "bar"}
      }}
    };
    auto copy = ys;
    merge(xs, copy, policy::merge_lists::yes);
    CHECK_EQUAL(copy, expected);
  }
  // clang-format on
}

TEST(strip) {
  auto xs = record{
    {"a", record{}},
    {"b", uint64_t{5u}},
    {"c",
     record{
       {"d",
        record{
          {"e", record{}},
          {"f", caf::none},
        }},
     }},
    {"g", caf::none},
  };
  auto expected = record{{"b", uint64_t{5u}}};
  CHECK_EQUAL(strip(xs), expected);
}

TEST(construction) {
  CHECK(is<caf::none_t>(data{}));
  CHECK(is<bool>(data{true}));
  CHECK(is<bool>(data{false}));
  CHECK(is<int64_t>(data{int64_t{0}}));
  CHECK(is<int64_t>(data{int64_t{42}}));
  CHECK(is<int64_t>(data{int64_t{-42}}));
  CHECK(is<uint64_t>(data{42u}));
  CHECK(is<double>(data{4.2}));
  CHECK(is<std::string>(data{"foo"}));
  CHECK(is<std::string>(data{std::string{"foo"}}));
  CHECK(is<pattern>(data{pattern{}}));
  CHECK(is<ip>(data{ip{}}));
  CHECK(is<subnet>(data{subnet{}}));
  CHECK(is<list>(data{list{}}));
  CHECK(is<map>(data{map{}}));
}

TEST(relational_operators) {
  data d1;
  data d2;
  CHECK(d1 == d2);
  CHECK(!(d1 < d2));
  CHECK(d1 <= d2);
  CHECK(d1 >= d2);
  CHECK(!(d1 > d2));

  d2 = int64_t{42};
  CHECK(d1 != d2);
  CHECK(d1 < d2);
  CHECK(d1 <= d2);
  CHECK(!(d1 >= d2));
  CHECK(!(d1 > d2));

  d1 = int64_t{42};
  d2 = caf::none;
  CHECK(d1 != d2);
  CHECK(!(d1 < d2));
  CHECK(!(d1 <= d2));
  CHECK(d1 >= d2);
  CHECK(d1 > d2);

  d2 = int64_t{1377};
  CHECK(d1 != d2);
  CHECK(d1 < d2);
  CHECK(d1 <= d2);
  CHECK(!(d1 >= d2));
  CHECK(!(d1 > d2));
}

TEST(evaluation) {
  MESSAGE("in");
  data lhs{"foo"};
  data rhs{"foobar"};
  CHECK(evaluate(lhs, relational_operator::in, rhs));
  CHECK(evaluate(rhs, relational_operator::not_in, lhs));
  CHECK(evaluate(rhs, relational_operator::ni, lhs));
  CHECK(evaluate(rhs, relational_operator::not_in, lhs));
  MESSAGE("equality");
  lhs = uint64_t{42};
  rhs = uint64_t{1337};
  CHECK(evaluate(lhs, relational_operator::less_equal, rhs));
  CHECK(evaluate(lhs, relational_operator::less, rhs));
  CHECK(evaluate(lhs, relational_operator::not_equal, rhs));
  CHECK(!evaluate(lhs, relational_operator::equal, rhs));
  MESSAGE("network types");
  lhs = *to<ip>("10.0.0.1");
  rhs = *to<subnet>("10.0.0.0/8");
  CHECK(evaluate(lhs, relational_operator::in, rhs));
  lhs = *to<subnet>("10.0.42.0/16");
  CHECK(evaluate(lhs, relational_operator::in, rhs));
  rhs = *to<subnet>("10.0.42.0/17");
  CHECK(!evaluate(lhs, relational_operator::in, rhs));
  MESSAGE("mixed types");
  rhs = double{4.2};
  CHECK(!evaluate(lhs, relational_operator::equal, rhs));
  CHECK(evaluate(lhs, relational_operator::not_equal, rhs));
}

TEST(evaluation - pattern matching) {
  CHECK(
    evaluate(unbox(to<pattern>("/f.*o/")), relational_operator::equal, "foo"));
  CHECK(
    evaluate("foo", relational_operator::equal, unbox(to<pattern>("/f.*o/"))));
  CHECK(
    evaluate(unbox(to<pattern>("/f.*o/i")), relational_operator::equal, "FOO"));
  CHECK(
    evaluate("FOO", relational_operator::equal, unbox(to<pattern>("/f.*o/i"))));
}

TEST(serialization) {
  list xs;
  xs.emplace_back(uint64_t{80});
  xs.emplace_back(uint64_t{53});
  xs.emplace_back(uint64_t{8});
  auto x0 = data{xs};
  caf::byte_buffer buf;
  CHECK(detail::serialize(buf, x0));
  data x1;
  CHECK_EQUAL(detail::legacy_deserialize(buf, x1), true);
  CHECK_EQUAL(x0, x1);
}

TEST(printable) {
  // Ensure that we don't produce trailing zeros for floating point data.
  auto x = data{-4.2};
  CHECK_EQUAL(to_string(x), "-4.2");
  x = 3.14;
  CHECK_EQUAL(to_string(x), "3.14");
}

TEST(parseable) {
  auto p = make_parser<data>();
  data d;
  MESSAGE("bool");
  auto str = "true"s;
  auto f = str.begin();
  auto l = str.end();
  CHECK(p(f, l, d));
  CHECK(f == l);
  CHECK(d == true);
  MESSAGE("numbers");
  str = "+1001"s;
  f = str.begin();
  l = str.end();
  CHECK(p(f, l, d));
  CHECK_EQUAL(f, l);
  CHECK_EQUAL(d, int64_t{1001});
  str = "1001"s;
  f = str.begin();
  l = str.end();
  CHECK(p(f, l, d));
  CHECK(f == l);
  CHECK(d == 1001u);
  str = "10.01"s;
  f = str.begin();
  l = str.end();
  CHECK(p(f, l, d));
  CHECK(f == l);
  CHECK(d == 10.01);
  MESSAGE("string");
  str = R"("bar")";
  f = str.begin();
  l = str.end();
  CHECK(p(f, l, d));
  CHECK(f == l);
  CHECK(d == "bar");
  MESSAGE("pattern");
  str = "/foo/"s;
  f = str.begin();
  l = str.end();
  CHECK(p(f, l, d));
  CHECK(f == l);
  CHECK(d == unbox(to<pattern>("/foo/")));
  MESSAGE("address");
  str = "10.0.0.1"s;
  f = str.begin();
  l = str.end();
  CHECK(p(f, l, d));
  CHECK(f == l);
  CHECK(d == *to<ip>("10.0.0.1"));
  MESSAGE("list");
  str = "[42,4.2,null]"s;
  f = str.begin();
  l = str.end();
  CHECK(p(f, l, d));
  CHECK(f == l);
  CHECK((d == list{42u, 4.2, caf::none}));
  MESSAGE("map");
  str = "{true->1,false->0}"s;
  f = str.begin();
  l = str.end();
  CHECK(p(f, l, d));
  CHECK(f == l);
  CHECK((d == map{{true, 1u}, {false, 0u}}));
}

TEST(convert - caf::config_value) {
  // clang-format off
  auto x = record{
    {"x", "foo"},
    {"r", record{
      {"i", int64_t{-42}},
      {"u", 42u},
      {"r", record{
        {"u", 3.14}
      }},
    }},
    {"delta", 12ms},
    {"uri", "https://tenzir.com/"},
    {"xs", list{int64_t{1}, int64_t{2}, int64_t{3}}},
    {"ys", list{int64_t{1}, "foo", 3.14}},
    {"zs", list{record{{"z", true}}, map{{42u, 4.2}}}}
  };
  // clang-format on
  using namespace caf;
  auto y = config_value::dictionary{};
  y.emplace("x", "foo");
  auto r = config_value::dictionary{};
  r.emplace("i", -42);
  r.emplace("u", 42u);
  auto rr = config_value::dictionary{};
  rr.emplace("u", 3.14);
  r.emplace("r", std::move(rr));
  y.emplace("r", std::move(r));
  y.emplace("delta", timespan{12ms});
  y.emplace("uri", "https://tenzir.com/"); // maybe in the future as caf::uri
  y.emplace("xs", make_config_value_list(1, 2, 3));
  y.emplace("ys", make_config_value_list(1, "foo", 3.14));
  auto z0 = config_value::dictionary{};
  z0.emplace("z", true);
  auto z1 = config_value::dictionary{};
  z1.emplace("42", 4.2);
  y.emplace("zs", make_config_value_list(std::move(z0), std::move(z1)));
  CHECK_EQUAL(unbox(to<settings>(x)), y);
  CHECK_EQUAL(unbox(to<dictionary<config_value>>(x)), y);
}

TEST(convert - caf::config_value - null) {
  // clang-format off
  auto x = record{
    {"valid", "foo"},
    {"invalid", caf::none}
  };
  // clang-format on
  using namespace caf;
  auto y = to<dictionary<config_value>>(x);
  REQUIRE(!y.engaged());
  CHECK_EQUAL(y.error(), ec::type_clash);
  // If we flatten the record first and weed out null values, it'll work.
  auto flat = flatten(x);
  auto& [k, v] = as_vector(flat).back();
  flat.erase(k);
  y = to<dictionary<config_value>>(flat);
  REQUIRE(y.engaged());
}

// We can't really test that a given call doesn't produce a stack overflow, so
// instead we test here that the fields that are nested deeper than
// `max_recursion_depth` are cut off during `flatten()`.
TEST(nesting depth) {
  auto x = record{{"leaf", int64_t{1}}};
  for (size_t i = 0; i < defaults::max_recursion; ++i) {
    auto tmp = record{{"nested", std::exchange(x, {})}};
    x = tmp;
  }
  auto final = record{{"branch1", x}, {"branch2", int64_t{4}}};
  CHECK_EQUAL(depth(final), defaults::max_recursion + 2);
  auto flattened = flatten(final);
  CHECK_EQUAL(depth(flattened), 1ull);
}

TEST(pack / unpack) {
  auto x = data{record{
    {"none", caf::none},
    {"bool", bool{true}},
    {"integer", int64_t{2}},
    {"count", uint64_t{3u}},
    {"real", double{4.0}},
    {"duration", duration{5}},
    {"time", tenzir::time{} + duration{6}},
    {"string", std::string{"7"}},
    {"pattern", unbox(to<pattern>("/7/"))},
    {"address", unbox(to<ip>("0.0.0.8"))},
    {"subnet", unbox(to<subnet>("0.0.0.9/24"))},
    {"enumeration", enumeration{10}},
    {"list", list{uint64_t{11}}},
    {"map", map{{std::string{"key"}, uint64_t{12}}}},
    {"record",
     record{
       {"nested_real", double{13.0}},
       {"nested_record", record{}},
     }},
  }};
  auto builder = flatbuffers::FlatBufferBuilder{};
  const auto offset = pack(builder, x);
  builder.Finish(offset);
  auto maybe_flatbuffer = flatbuffer<fbs::Data>::make(builder.Release());
  REQUIRE_NOERROR(maybe_flatbuffer);
  auto flatbuffer = *maybe_flatbuffer;
  REQUIRE(maybe_flatbuffer);
  auto x2 = data{};
  REQUIRE_EQUAL(unpack(*flatbuffer, x2), caf::none);
  CHECK_EQUAL(x, x2);
}

TEST(get_if) {
  // clang-format off
  auto x = record{
    {"foo", "bar"},
    {"baz", record{
      {"qux", int64_t{42}},
      {"quux", record{
        {"quuux", 3.14}
      }},
    }},
  };
  // clang-format on
  auto foo = get_if<std::string>(&x, "foo");
  REQUIRE(foo);
  CHECK_EQUAL(*foo, "bar");
  auto invalid = get_if<ip>(&x, "foo");
  CHECK(!invalid);
  auto baz = get_if<record>(&x, "baz");
  CHECK(baz);
  auto qux = get_if<int64_t>(&x, "baz.qux");
  REQUIRE(qux);
  REQUIRE(*qux);
  CHECK_EQUAL(*qux, 42);
  auto quux = get_if<record>(&x, "baz.quux");
  CHECK(quux);
  auto quuux = get_if<double>(&x, "baz.quux.quuux");
  REQUIRE(quuux);
  CHECK_EQUAL(*quuux, 3.14);
  auto unknown = get_if<ip>(&x, "foo.baz");
  CHECK(!unknown);
}

TEST(get_or) {
  auto x = record{{"foo", "bar"}};
  auto fallback = std::string{"fallback"};
  auto foo = get_or(x, "foo", fallback);
  CHECK_EQUAL(foo, "bar");
  auto bar = get_or(x, "bar", fallback);
  CHECK_EQUAL(bar, "fallback");
  auto qux = get_or(x, "qux", "literal");
  CHECK_EQUAL(qux, "literal");
}

//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/arrow_table_slice_builder.hpp"
#define SUITE view

#include "vast/test/test.hpp"
#include "vast/type.hpp"
#include "vast/view.hpp"

#include <arrow/scalar.h>

using namespace vast;
using namespace std::literals;

TEST(copying views) {
  MESSAGE("calling view directly");
  CHECK_VARIANT_EQUAL(view<caf::none_t>{caf::none}, caf::none);
  CHECK_VARIANT_EQUAL(view<bool>{true}, true);
  CHECK_VARIANT_EQUAL(view<integer>{42}, integer{42});
  CHECK_VARIANT_EQUAL(view<count>{42}, 42u);
  CHECK_VARIANT_EQUAL(view<real>{4.2}, 4.2);
  MESSAGE("using make_view");
  CHECK_VARIANT_EQUAL(make_view(caf::none), caf::none);
  CHECK_VARIANT_EQUAL(make_view(true), true);
  CHECK_VARIANT_EQUAL(make_view(integer{42}), integer{42});
  CHECK_VARIANT_EQUAL(make_view(42u), count(42u));
  CHECK_VARIANT_EQUAL(make_view(4.2), real(4.2));
  MESSAGE("copying from temporary data");
  CHECK_VARIANT_EQUAL(make_view(data{caf::none}), caf::none);
  CHECK_VARIANT_EQUAL(make_view(data{true}), true);
  CHECK_VARIANT_EQUAL(make_view(data{integer{42}}), integer{42});
  CHECK_VARIANT_EQUAL(make_view(data{42u}), count(42u));
  CHECK_VARIANT_EQUAL(make_view(data{4.2}), real(4.2));
}

TEST(string literal view) {
  auto v = make_view("foobar");
  CHECK_EQUAL(v.size(), 6u);
  CHECK_EQUAL(v, "foobar"sv);
  CHECK_EQUAL(std::string{"foobar"}, materialize(v));
}

TEST(string view) {
  auto str = "foobar"s;
  auto v = make_view(str);
  CHECK_EQUAL(v, "foobar");
  str[3] = 'z';
  CHECK_EQUAL(v, "foozar");
  CHECK_EQUAL(str, materialize(v));
}

TEST(list view) {
  auto xs = list{integer{42}, true, "foo", 4.2};
  auto v = make_view(xs);
  REQUIRE_EQUAL(v->size(), xs.size());
  auto i = v->begin();
  CHECK_EQUAL(*i, v->at(0));
  CHECK_EQUAL(*i, make_data_view(integer{42}));
  ++i;
  CHECK_EQUAL(*i, v->at(1));
  CHECK_EQUAL(*i, make_data_view(true));
  i += 2;
  CHECK_EQUAL(*i, v->at(3));
  CHECK_EQUAL(*i, make_data_view(4.2));
  ++i;
  CHECK_EQUAL(i, v->end());
  auto j = v->begin() + 1;
  CHECK_EQUAL(i - j, xs.size() - 1);
  MESSAGE("check conversion back to data");
  CHECK_EQUAL(xs, materialize(v));
}

TEST(map view) {
  auto xs = map{{integer{42}, true}, {integer{84}, false}};
  auto v = make_view(xs);
  REQUIRE_EQUAL(v->size(), xs.size());
  MESSAGE("check view contents");
  for (auto i = 0u; i < xs.size(); ++i) {
    auto [key, value] = v->at(i);
    auto& [expected_key, expected_value] = *std::next(xs.begin(), i);
    CHECK_EQUAL(key, make_data_view(expected_key));
    CHECK_EQUAL(value, make_data_view(expected_value));
  }
  MESSAGE("check iterator behavior");
  CHECK_EQUAL(std::next(v->begin(), 2), v->end());
  MESSAGE("check iterator value type");
  auto [key, value] = *v->begin();
  CHECK_EQUAL(key, make_data_view(integer{42}));
  CHECK_EQUAL(value, make_data_view(true));
  MESSAGE("check conversion back to data");
  CHECK_EQUAL(xs, materialize(v));
}

TEST(make_data_view) {
  auto x = make_data_view(true);
  CHECK(caf::holds_alternative<bool>(x));
  auto str = "foo"s;
  x = make_data_view(str);
  CHECK(caf::holds_alternative<view<std::string>>(x));
  CHECK(caf::holds_alternative<std::string_view>(x));
  auto xs = list{integer{42}, true, "foo"};
  x = make_data_view(xs);
  REQUIRE(caf::holds_alternative<view<list>>(x));
  auto v = caf::get<view<list>>(x);
  REQUIRE_EQUAL(v->size(), 3u);
  CHECK_VARIANT_EQUAL(v->at(0), integer{42});
  CHECK_VARIANT_EQUAL(v->at(1), true);
  CHECK_VARIANT_EQUAL(v->at(2), "foo"sv);
  CHECK_EQUAL(xs, materialize(v));
}

TEST(comparison with data) {
  auto x = data{true};
  auto y = make_view(x);
  CHECK(is_equal(x, y));
  CHECK(is_equal(y, x));
  y = make_data_view(false);
  CHECK(!is_equal(x, y));
  y = caf::none;
  CHECK(!is_equal(x, y));
  x = caf::none;
  CHECK(is_equal(x, y));
  x = list{integer{1}, "foo", 4.2};
  y = make_view(x);
  CHECK(is_equal(x, y));
}

TEST(increment decrement container_view_iterator) {
  auto xs = list{integer{42}, true, "foo", 4.2};
  auto v = make_view(xs);
  auto it1 = v->begin();
  auto it2 = v->begin();
  CHECK_EQUAL(it1.distance_to(it2), 0u);
  ++it1;
  CHECK_NOT_EQUAL(it1.distance_to(it2), 0u);
  --it1;
  CHECK_EQUAL(it1.distance_to(it2), 0u);
}

TEST(container comparison) {
  data xs = list{integer{42}};
  data ys = list{integer{42}};
  CHECK(make_view(xs) == make_view(ys));
  CHECK(!(make_view(xs) < make_view(ys)));
  caf::get<list>(ys).push_back(integer{0});
  CHECK(make_view(xs) != make_view(ys));
  CHECK(make_view(xs) < make_view(ys));
  ys = map{{integer{42}, true}};
  CHECK(make_view(xs) != make_view(ys));
  CHECK(make_view(xs) < make_view(ys));
  xs = map{{integer{43}, true}};
  CHECK(make_view(xs) > make_view(ys));
}

TEST(hashing views) {
  data i = integer{1};
  data c = "chars";
  data s = "string"s;
  data p = pattern{"x"};
  data v = list{integer{42}, true, "foo", 4.2};
  data m = map{{integer{42}, true}, {integer{84}, false}};
  data r = record{{"foo", integer{42}}, {"bar", true}};
  auto h = [](auto&& x) {
    return hash<xxh64>(x);
  };
  CHECK_EQUAL(h(i), h(make_view(i)));
  CHECK_EQUAL(h(c), h(make_view(c)));
  CHECK_EQUAL(h(s), h(make_view(s)));
  CHECK_EQUAL(h(p), h(make_view(p)));
  CHECK_EQUAL(h(v), h(make_view(v)));
  CHECK_EQUAL(h(m), h(make_view(m)));
  CHECK_EQUAL(h(r), h(make_view(r)));
  using stdhash = std::hash<data>;
  CHECK_EQUAL(stdhash{}(i), stdhash{}(make_view(i)));
  CHECK_EQUAL(stdhash{}(c), stdhash{}(make_view(c)));
  CHECK_EQUAL(stdhash{}(s), stdhash{}(make_view(s)));
  CHECK_EQUAL(stdhash{}(p), stdhash{}(make_view(p)));
  CHECK_EQUAL(stdhash{}(v), stdhash{}(make_view(v)));
  CHECK_EQUAL(stdhash{}(m), stdhash{}(make_view(m)));
  CHECK_EQUAL(stdhash{}(r), stdhash{}(make_view(r)));
}

TEST(arrow scalar conversion) {
  // A naive implementation of to_arrow_scalar that is easy to verify and used
  // for producing the expected values.
  auto naive_to_arrow_scalar = [](const type& type, const data_view& view) {
    REQUIRE(type_check(type, view));
    auto builder = type.make_arrow_builder(arrow::default_memory_pool());
    REQUIRE(builder);
    auto append_result = append_builder(type, *builder, view);
    if (!append_result.ok())
      FAIL(append_result.ToString());
    auto array = builder->Finish();
    if (!array.ok())
      FAIL(array.status().ToString());
    auto scalar = array.MoveValueUnsafe()->GetScalar(0);
    if (!scalar.ok())
      FAIL(scalar.status().ToString());
    return scalar.MoveValueUnsafe();
  };
  auto check = [&](const type& type, const data_view& view) {
    MESSAGE("checking type " << fmt::to_string(type) << " with data "
                             << fmt::to_string(view));
    const auto expected = naive_to_arrow_scalar(type, view);
    const auto value = to_arrow_scalar(type, view);
    REQUIRE(expected);
    REQUIRE(value);
    CHECK(expected->Equals(*value));
  };
  // TODO: Implement such a check for all types.
  check(type{bool_type{}}, true);
  check(type{bool_type{}}, false);
  check(type{integer_type{}}, integer{42});
  check(type{count_type{}}, count{42});
  check(type{record_type{{"foo", string_type{}}}},
        make_data_view(record{{"foo", "123"}}));
  check(type{string_type{}}, "123"sv);
  check(type{string_type{}}, "123"sv);
}

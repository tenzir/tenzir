//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/view.hpp"

#include "tenzir/concept/parseable/tenzir/pattern.hpp"
#include "tenzir/concept/parseable/to.hpp"
#include "tenzir/test/test.hpp"

#include <caf/test/dsl.hpp>

using namespace tenzir;
using namespace std::literals;

TEST(copying views) {
  MESSAGE("calling view directly");
  CHECK_VARIANT_EQUAL(view<caf::none_t>{caf::none}, caf::none);
  CHECK_VARIANT_EQUAL(view<bool>{true}, true);
  CHECK_VARIANT_EQUAL(view<int64_t>{42}, int64_t{42});
  CHECK_VARIANT_EQUAL(view<uint64_t>{42}, 42u);
  CHECK_VARIANT_EQUAL(view<double>{4.2}, 4.2);
  MESSAGE("using make_view");
  CHECK_VARIANT_EQUAL(make_view(caf::none), caf::none);
  CHECK_VARIANT_EQUAL(make_view(true), true);
  CHECK_VARIANT_EQUAL(make_view(int64_t{42}), int64_t{42});
  CHECK_VARIANT_EQUAL(materialize(make_view(42u)), uint64_t(42u));
  CHECK_VARIANT_EQUAL(make_view(4.2), double(4.2));
  MESSAGE("copying from temporary data");
  CHECK_VARIANT_EQUAL(materialize(make_view(data{caf::none})), caf::none);
  CHECK_VARIANT_EQUAL(materialize(make_view(data{true})), true);
  CHECK_VARIANT_EQUAL(materialize(make_view(data{int64_t{42}})), int64_t{42});
  CHECK_VARIANT_EQUAL(materialize(make_view(data{42u})), uint64_t(42u));
  CHECK_VARIANT_EQUAL(materialize(make_view(data{4.2})), double(4.2));
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
  auto xs = list{int64_t{42}, true, "foo", 4.2};
  auto v = make_view(xs);
  REQUIRE_EQUAL(v->size(), xs.size());
  auto i = v->begin();
  CHECK_EQUAL(materialize(*i), materialize(v->at(0)));
  CHECK_EQUAL(materialize(*i), int64_t{42});
  ++i;
  CHECK_EQUAL(materialize(*i), materialize(v->at(1)));
  CHECK_EQUAL(materialize(*i), true);
  i += 2;
  CHECK_EQUAL(materialize(*i), materialize(v->at(3)));
  CHECK_EQUAL(materialize(*i), 4.2);
  ++i;
  CHECK_EQUAL(i, v->end());
  auto j = v->begin() + 1;
  CHECK_EQUAL(i - j, xs.size() - 1);
  MESSAGE("check conversion back to data");
  CHECK_EQUAL(xs, materialize(v));
}

TEST(map view) {
  auto xs = map{{int64_t{42}, true}, {int64_t{84}, false}};
  auto v = make_view(xs);
  REQUIRE_EQUAL(v->size(), xs.size());
  MESSAGE("check view contents");
  for (auto i = 0u; i < xs.size(); ++i) {
    auto [key, value] = v->at(i);
    auto& [expected_key, expected_value] = *std::next(xs.begin(), i);
    CHECK_EQUAL(materialize(key), expected_key);
    CHECK_EQUAL(materialize(value), expected_value);
  }
  MESSAGE("check iterator behavior");
  CHECK_EQUAL(std::next(v->begin(), 2), v->end());
  MESSAGE("check iterator value type");
  auto [key, value] = *v->begin();
  CHECK_EQUAL(materialize(key), int64_t{42});
  CHECK_EQUAL(materialize(value), true);
  MESSAGE("check conversion back to data");
  CHECK_EQUAL(xs, materialize(v));
}

TEST(make_data_view) {
  auto x = make_data_view(true);
  CHECK(is<bool>(x));
  auto str = "foo"s;
  x = make_data_view(str);
  CHECK(is<view<std::string>>(x));
  CHECK(is<std::string_view>(x));
  auto xs = list{int64_t{42}, true, "foo"};
  x = make_data_view(xs);
  REQUIRE(is<view<list>>(x));
  auto v = as<view<list>>(x);
  REQUIRE_EQUAL(v->size(), 3u);
  CHECK_VARIANT_EQUAL(materialize(v->at(0)), int64_t{42});
  CHECK_VARIANT_EQUAL(materialize(v->at(1)), true);
  CHECK_VARIANT_EQUAL(materialize(v->at(2)), "foo");
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
  x = list{int64_t{1}, "foo", 4.2};
  y = make_view(x);
  CHECK(is_equal(x, y));
}

TEST(increment decrement container_view_iterator) {
  auto xs = list{int64_t{42}, true, "foo", 4.2};
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
  data xs = list{int64_t{42}};
  data ys = list{int64_t{42}};
  CHECK(make_view(xs) == make_view(ys));
  CHECK(!(make_view(xs) < make_view(ys)));
  as<list>(ys).push_back(int64_t{0});
  CHECK(make_view(xs) != make_view(ys));
  CHECK(make_view(xs) < make_view(ys));
  ys = map{{int64_t{42}, true}};
  CHECK(make_view(xs) != make_view(ys));
  CHECK(make_view(xs) < make_view(ys));
  xs = map{{int64_t{43}, true}};
  CHECK(make_view(xs) > make_view(ys));
}

TEST(hashing views) {
  data i = int64_t{1};
  data c = "chars";
  data s = "string"s;
  data p = unbox(to<pattern>("/x/"));
  data v = list{int64_t{42}, true, "foo", 4.2};
  data m = map{{int64_t{42}, true}, {int64_t{84}, false}};
  data r = record{{"foo", int64_t{42}}, {"bar", true}};
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

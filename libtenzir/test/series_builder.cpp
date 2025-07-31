//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/series_builder.hpp"

#include "tenzir/fwd.hpp"

#include "tenzir/table_slice.hpp"
#include "tenzir/test/test.hpp"

#include <arrow/api.h>

#include <string_view>

namespace tenzir {

namespace {

void finish_and_check(
  series_builder& b,
  const std::vector<std::tuple<int64_t, std::string_view, std::string_view>>&
    ref) {
  auto total = int64_t{0};
  for (auto& r : ref) {
    total += std::get<0>(r);
  }
  CHECK_EQUAL(b.length(), total);
  auto result = b.finish();
  CHECK_EQUAL(result.size(), ref.size());
  for (auto i = size_t{0}; i < result.size(); ++i) {
    auto [len, ty, data] = i < ref.size() ? ref[i] : decltype(ref[i]){};
    CHECK_EQUAL(result[i].array->length(), len);
    CHECK_EQUAL(result[i].array->type()->ToString(), ty);
    auto string = result[i].array->ToString();
    // For whatever reason, arrow likes to add null bytes into the string.
    std::erase(string, '\0');
    CHECK_EQUAL(string, data);
  }
  CHECK_EQUAL(b.length(), 0);
}

TEST("series builder empty") {
  auto b = series_builder{};
  finish_and_check(b, {});
}

TEST("one empty record") {
  auto b = series_builder{};
  b.record();
  finish_and_check(b, {{1, R"(struct<>)", R"(-- is_valid: all not null)"}});
}

TEST("two empty records") {
  auto b = series_builder{};
  b.record();
  b.record();
  finish_and_check(b, {{2, R"(struct<>)", R"(-- is_valid: all not null)"}});
}

TEST("one null") {
  auto b = series_builder{};
  b.null();
  finish_and_check(b, {{1, R"(null)", R"(1 nulls)"}});
}

TEST("two nulls") {
  auto b = series_builder{};
  b.null();
  b.null();
  finish_and_check(b, {{2, R"(null)", R"(2 nulls)"}});
}

TEST("one empty record then one null") {
  auto b = series_builder{};
  b.record();
  b.null();
  finish_and_check(b, {{2, R"(struct<>)", R"(-- is_valid:
  [
    true,
    false
  ])"}});
}

TEST("one null then one empty record") {
  auto b = series_builder{};
  b.null();
  b.record();
  finish_and_check(b, {{2, R"(struct<>)", R"(-- is_valid:
  [
    false,
    true
  ])"}});
}

TEST("one record with one field") {
  auto b = series_builder{};
  b.record().field("a").data(42);
  finish_and_check(b, {{1, R"(struct<a: int64>)", R"(-- is_valid: all not null
-- child 0 type: int64
  [
    42
  ])"}});
}

TEST("one nested record then a null") {
  auto b = series_builder{};
  b.record().field("a").record().field("b").data(42);
  b.null();
  finish_and_check(b, {{2, R"(struct<a: struct<b: int64>>)", R"(-- is_valid:
  [
    true,
    false
  ]
-- child 0 type: struct<b: int64>
  -- is_valid:
      [
      true,
      false
    ]
  -- child 0 type: int64
    [
      42,
      null
    ])"}});
}

TEST("one nested record then one empty record") {
  auto b = series_builder{};
  b.record().field("a").record().field("b").data(42);
  b.record();
  finish_and_check(
    b, {{2, R"(struct<a: struct<b: int64>>)", R"(-- is_valid: all not null
-- child 0 type: struct<b: int64>
  -- is_valid:
      [
      true,
      false
    ]
  -- child 0 type: int64
    [
      42,
      null
    ])"}});
}

TEST("two nested records") {
  auto b = series_builder{};
  b.record().field("a").record().field("b").data(42);
  b.record().field("a").record().field("b").data(43);
  finish_and_check(
    b, {{2, R"(struct<a: struct<b: int64>>)", R"(-- is_valid: all not null
-- child 0 type: struct<b: int64>
  -- is_valid: all not null
  -- child 0 type: int64
    [
      42,
      43
    ])"}});
}

TEST("set field to value then to null") {
  auto b = series_builder{};
  auto foo = b.record().field("foo");
  foo.data(42);
  foo.null();
  finish_and_check(b, {{1, "struct<foo: null>", R"(-- is_valid: all not null
-- child 0 type: null
1 nulls)"}});
}

TEST("set field to null then to value") {
  auto b = series_builder{};
  auto foo = b.record().field("foo");
  foo.null();
  foo.data(42);
  finish_and_check(b, {{1, "struct<foo: int64>", R"(-- is_valid: all not null
-- child 0 type: int64
  [
    42
  ])"}});
}

TEST("set field to int64 then to other int64") {
  auto b = series_builder{};
  auto foo = b.record().field("foo");
  foo.data(42);
  foo.data(43);
  finish_and_check(b, {{1, "struct<foo: int64>", R"(-- is_valid: all not null
-- child 0 type: int64
  [
    43
  ])"}});
}

TEST("set field to list then to other list") {
  auto b = series_builder{};
  b.record().field("foo").list().data(0);
  auto foo = b.record().field("foo");
  auto x = foo.list();
  x.data(1);
  x.data(2);
  auto y = foo.list();
  y.data(3);
  finish_and_check(
    b, {{2, "struct<foo: list<item: int64>>", R"(-- is_valid: all not null
-- child 0 type: list<item: int64>
  [
    [
      0
    ],
    [
      3
    ]
  ])"}});
}

TEST("top level conflicting types") {
  auto b = series_builder{};
  b.record();
  b.data(42);
  b.data(43);
  b.record().field("foo").data(44);
  b.null();
  b.data(45);
  finish_and_check(b, {{1, "struct<>", R"(-- is_valid: all not null)"},
                       {2, "int64", R"([
  42,
  43
])"},
                       {2, "struct<foo: int64>", R"(-- is_valid:
  [
    true,
    false
  ]
-- child 0 type: int64
  [
    44,
    null
  ])"},
                       {1, "int64", R"([
  45
])"}});
}

TEST("conflict in first record field") {
  auto b = series_builder{};
  b.record().field("foo").data(42);
  b.record().field("foo").data(43);
  b.record().field("foo").record();
  finish_and_check(b, {{2, "struct<foo: int64>", R"(-- is_valid: all not null
-- child 0 type: int64
  [
    42,
    43
  ])"},
                       {1, "struct<foo: struct<>>", R"(-- is_valid: all not null
-- child 0 type: struct<>
  -- is_valid: all not null)"}});
}

TEST("conflict in second record field") {
  auto b = series_builder{};
  auto r = b.record();
  r.field("foo").data(1);
  r.field("bar").data(2);
  r = b.record();
  r.field("foo").data(3);
  r.field("bar").record();
  finish_and_check(
    b, {{1, "struct<foo: int64, bar: int64>", R"(-- is_valid: all not null
-- child 0 type: int64
  [
    1
  ]
-- child 1 type: int64
  [
    2
  ])"},
        {1, "struct<foo: int64, bar: struct<>>", R"(-- is_valid: all not null
-- child 0 type: int64
  [
    3
  ]
-- child 1 type: struct<>
  -- is_valid: all not null)"}});
}

TEST("conflict with list from previous event") {
  auto b = series_builder{};
  auto l = b.record().field("foo").list();
  l.data(1);
  l.data(2);
  l = b.record().field("foo").list();
  l.record().field("bar").data(3);
  l.record().field("bar").data(4);
  finish_and_check(
    b, {{1, "struct<foo: list<item: int64>>", R"(-- is_valid: all not null
-- child 0 type: list<item: int64>
  [
    [
      1,
      2
    ]
  ])"},
        {1, "struct<foo: list<item: struct<bar: int64>>>",
         R"(-- is_valid: all not null
-- child 0 type: list<item: struct<bar: int64>>
  [
    -- is_valid: all not null
    -- child 0 type: int64
      [
        3,
        4
      ]
  ])"}});
}

TEST("conflict with list from current event") {
  auto b = series_builder{};
  auto l = b.record().field("foo").list();
  l.data(1);
  l.data(2);
  l.record().field("bar").data(3);
  finish_and_check(
    b, {{1, "struct<foo: list<item: string>>", R"(-- is_valid: all not null
-- child 0 type: list<item: string>
  [
    [
      "1",
      "2",
      "{"bar":3}"
    ]
  ])"}});
}

TEST("conflict with list from current event then no conflict") {
  auto b = series_builder{};
  auto l = b.record().field("foo").list();
  l.data(1);
  l.data(2);
  l.record().field("bar").data(3);
  b.record().field("foo").list().record().field("bar").data(4);
  b.record().field("foo").list().record().field("bar").data(5);
  finish_and_check(
    b, {{1, "struct<foo: list<item: string>>", R"(-- is_valid: all not null
-- child 0 type: list<item: string>
  [
    [
      "1",
      "2",
      "{"bar":3}"
    ]
  ])"},
        {2, "struct<foo: list<item: struct<bar: int64>>>",
         R"(-- is_valid: all not null
-- child 0 type: list<item: struct<bar: int64>>
  [
    -- is_valid: all not null
    -- child 0 type: int64
      [
        4
      ],
    -- is_valid: all not null
    -- child 0 type: int64
      [
        5
      ]
  ])"}});
}

TEST("conflict with list from current event but nested") {
  auto b = series_builder{};
  auto l = b.record().field("foo").list();
  l.record().field("bar").data(1);
  l.record().field("bar").list().data(2);
  finish_and_check(b, {{1, "struct<foo: list<item: struct<bar: string>>>",
                        R"(-- is_valid: all not null
-- child 0 type: list<item: struct<bar: string>>
  [
    -- is_valid: all not null
    -- child 0 type: string
      [
        "1",
        "[2]"
      ]
  ])"}});
}

TEST("conflict with list from current event within another conflict") {
  auto b = series_builder{};
  auto l = b.list();
  l.data(1);
  l.record().field("foo").data(2);
  l.record().field("foo").record().field("bar").data(3);
  finish_and_check(b, {{1, "list<item: string>", R"([
  [
    "1",
    "{"foo":"2"}",
    "{"foo":"{\"bar\":3}"}"
  ]
])"}});
}

TEST("to table slice") {
  auto b = series_builder{};
  b.record().field("foo").data(42);
  auto ip = ip::v4(0xABCD1234);
  b.record().field("bar").list().data(ip);
  auto slices = b.finish_as_table_slice("hi");
  REQUIRE_EQUAL(slices.size(), size_t{1});
  auto& slice = slices[0];
  REQUIRE_EQUAL(slice.rows(), uint64_t{2});
  CHECK_EQUAL(materialize(slice.at(0, 0)), int64_t{42});
  CHECK_EQUAL(materialize(slice.at(0, 1)), caf::none);
  CHECK_EQUAL(materialize(slice.at(1, 0)), caf::none);
  CHECK_EQUAL(materialize(slice.at(1, 1)), list{ip});
}

TEST("enumeration type") {
  // TODO: API for string/enum?
  // TODO: How to handle invalid enum entry? Existing builder appends null
  // without warning.
  auto t = type{record_type{{"foo", enumeration_type{{"bar", 0}, {"baz", 1}}}}};
  auto b = series_builder{t};
  b.record().field("foo").data(enumeration{0});
  b.record().field("foo").data(caf::none);
  b.record().field("foo").data(enumeration{1});
  finish_and_check(b, {{3, "struct<foo: extension<tenzir.enumeration>>",
                        R"(-- is_valid: all not null
-- child 0 type: extension<tenzir.enumeration>

  -- dictionary:
    [
      "bar",
      "baz"
    ]
  -- indices:
    [
      0,
      null,
      1
    ])"}});
}

TEST("playground") {
  auto b = series_builder{};
  b.data(1);
  b.data(2.3);
  b.data(ip::v4(0xDEADBEEF));
  b.data(subnet{ip::v4(0x99C0FFEE), 123});
  finish_and_check(b,
                   {{1, "int64", R"([
  1
])"},
                    {1, "double", R"([
  2.3
])"},
                    {1, "extension<tenzir.ip>", R"([
  00000000000000000000FFFFDEADBEEF
])"},
                    {1, "extension<tenzir.subnet>", R"(-- is_valid: all not null
-- child 0 type: extension<tenzir.ip>
  [
    00000000000000000000FFFF99C0FFE0
  ]
-- child 1 type: uint8
  [
    123
  ])"}});
}

} // namespace

} // namespace tenzir

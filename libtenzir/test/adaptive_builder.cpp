//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/adaptive_builder.hpp"

#include "tenzir/test/test.hpp"

#include <arrow/api.h>

namespace tenzir {

using namespace experimental;

namespace {

void check(series_builder& b, int64_t length, std::string_view type,
           std::string_view array) {
  CHECK_EQUAL(b.length(), length);
  auto a = b.finish();
  CHECK_EQUAL(a->length(), length);
  CHECK_EQUAL(a->type()->ToString(), type);
  CHECK_EQUAL(a->ToString(), array);
}

TEST(empty) {
  auto b = series_builder{};
  check(b, 0, R"(null)", R"(0 nulls)");
}

TEST(one empty record) {
  auto b = series_builder{};
  b.record();
  check(b, 1, R"(struct<>)", R"(-- is_valid: all not null)");
}

TEST(two empty records) {
  auto b = series_builder{};
  b.record();
  b.record();
  check(b, 2, R"(struct<>)", R"(-- is_valid: all not null)");
}

TEST(one null) {
  auto b = series_builder{};
  b.null();
  check(b, 1, R"(null)", R"(1 nulls)");
}

TEST(two nulls) {
  auto b = series_builder{};
  b.null();
  b.null();
  check(b, 2, R"(null)", R"(2 nulls)");
}

TEST(one empty record then one null) {
  auto b = series_builder{};
  b.record();
  b.null();
  check(b, 2, R"(struct<>)", R"(-- is_valid:
  [
    true,
    false
  ])");
}

TEST(one null then one empty record) {
  auto b = series_builder{};
  b.null();
  b.record();
  check(b, 2, R"(struct<>)", R"(-- is_valid:
  [
    false,
    true
  ])");
}

TEST(one record with one field) {
  auto b = series_builder{};
  b.record().field("a").atom(42);
  check(b, 1, R"(struct<a: int64>)", R"(-- is_valid: all not null
-- child 0 type: int64
  [
    42
  ])");
}

TEST(one nested record then a null) {
  auto b = series_builder{};
  b.record().field("a").record().field("b").atom(42);
  b.null();
  check(b, 2, R"(struct<a: struct<b: int64>>)", R"(-- is_valid:
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
    ])");
}

TEST(one nested record then one empty record) {
  auto b = series_builder{};
  b.record().field("a").record().field("b").atom(42);
  b.record();
  check(b, 2, R"(struct<a: struct<b: int64>>)", R"(-- is_valid: all not null
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
    ])");
}

TEST(two nested records) {
  auto b = series_builder{};
  b.record().field("a").record().field("b").atom(42);
  b.record().field("a").record().field("b").atom(43);
  check(b, 2, R"(struct<a: struct<b: int64>>)", R"(-- is_valid: all not null
-- child 0 type: struct<b: int64>
  -- is_valid: all not null
  -- child 0 type: int64
    [
      42,
      43
    ])");
}

TEST(challenge) {
  auto b = series_builder{};
  b.record().field("a").record();
  b.record().field("a").atom(42);
  b.list().atom(43);
  b.record().field("a").atom(44);
  check(
    b, 4,
    R"(dense_union<0: struct<a: dense_union<: struct<>=0, : int64=1>>=0, 1: list<item: int64>=1>)",
    R"(-- is_valid: all not null
-- type_ids:   [
    0,
    0,
    1,
    0
  ]
-- value_offsets:   [
    0,
    1,
    0,
    2
  ]
-- child 0 type: struct<a: dense_union<: struct<>=0, : int64=1>>
  -- is_valid: all not null
  -- child 0 type: dense_union<0: struct<>=0, 1: int64=1>
    -- is_valid: all not null
    -- type_ids:       [
        0,
        1,
        1
      ]
    -- value_offsets:       [
        0,
        0,
        1
      ]
    -- child 0 type: struct<>
      -- is_valid: all not null
    -- child 1 type: int64
      [
        42,
        44
      ]
-- child 1 type: list<item: int64>
  [
    [
      43
    ]
  ])");
}

} // namespace
} // namespace tenzir

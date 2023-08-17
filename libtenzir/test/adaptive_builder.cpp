//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/adaptive_builder.hpp"

#include "tenzir/test/test.hpp"

namespace tenzir {
namespace {

auto test = R"__(

{a: "hello"}
{a: 42}
{a: {b: 43}}

Union all three??

{a: [{b: 42}, {b: {c: 43}}]}

)__";

TEST(union builder) {
  // [int64] -> [int64 | uint64]
  auto b = std::make_shared<arrow::Int64Builder>();
  auto x = arrow::ListBuilder{arrow::default_memory_pool(), b};
  (void)x.Append();
  (void)b->Append(123);
  (void)b->Append(456);
  (void)x.Append();
  auto ub = union_builder{std::move(b)};
  auto c = std::make_shared<arrow::UInt64Builder>();
  ub.add_variant(c);
  ub.add_next(1);
  (void)c->Append(31413);
  auto result = ub.finish();
  CHECK_EQUAL(result->ToString(), R"(-- is_valid: all not null
-- type_ids:   [
    0,
    0,
    1
  ]
-- value_offsets:   [
    0,
    1,
    0
  ]
-- child 0 type: int64
  [
    123,
    456
  ]
-- child 1 type: uint64
  [
    31413
  ])");

  // auto b = adaptive_builder{}; // list[?]
  // auto row = b.push_row();
  // row.set_field("yo", 42);
  // row.set_field("ok", 43);
  // auto result = b.finish();
}

} // namespace
} // namespace tenzir

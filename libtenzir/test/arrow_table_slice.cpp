//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/arrow_table_slice.hpp"

#include "tenzir/arrow_memory_pool.hpp"
#include "tenzir/arrow_utils.hpp"
#include "tenzir/test/test.hpp"

#include <arrow/api.h>
#include <arrow/util/bit_util.h>

#include <cstring>

namespace tenzir {

TEST("record batch from struct array preserves row nulls") {
  auto int_builder = arrow::Int64Builder{arrow_memory_pool()};
  check(int_builder.Append(int64_t{42}));
  check(int_builder.Append(int64_t{1337}));
  auto ints = finish(int_builder);
  auto string_builder = arrow::StringBuilder{arrow_memory_pool()};
  check(string_builder.Append("keep"));
  check(string_builder.Append("leak"));
  auto strings = finish(string_builder);
  auto bitmap = check(arrow::AllocateBitmap(2, arrow_memory_pool()));
  std::memset(bitmap->mutable_data(), 0, bitmap->size());
  arrow::bit_util::SetBit(bitmap->mutable_data(), 0);
  auto fields = arrow::FieldVector{
    arrow::field("a", arrow::int64()),
    arrow::field("b", arrow::utf8()),
  };
  auto struct_array = make_struct_array(2, bitmap, fields, {ints, strings});
  auto batch
    = record_batch_from_struct_array(arrow::schema(fields), struct_array);
  REQUIRE(batch);
  auto a
    = std::dynamic_pointer_cast<arrow::Int64Array>(batch->GetColumnByName("a"));
  auto b = std::dynamic_pointer_cast<arrow::StringArray>(
    batch->GetColumnByName("b"));
  REQUIRE(a);
  REQUIRE(b);
  CHECK(not a->IsNull(0));
  CHECK_EQUAL(a->Value(0), int64_t{42});
  CHECK(not b->IsNull(0));
  CHECK_EQUAL(b->GetString(0), "keep");
  CHECK(a->IsNull(1));
  CHECK(b->IsNull(1));
}

} // namespace tenzir

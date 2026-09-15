//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/nova/array_builder.hpp"
#include "tenzir/nova/storage.hpp"
#include "tenzir/nova/type_id.hpp"
#include "tenzir/test/test.hpp"

#include <string>
#include <string_view>

using namespace tenzir::nova;

namespace {

auto id(Array<Data> const& array, storage::Index row = 0) -> std::string {
  auto mask = storage::BitMap::Mutable{array.length()};
  mask.set(row, true);
  auto result = type_id(array, std::move(mask).finish());
  return std::string{*result.get(row)};
}

} // namespace

TEST("type ids ignore physical storage") {
  auto builder = ArrayBuilder<Int>{};
  builder.data(std::int64_t{42});
  auto primary = Array<Data>{builder.finish()};
  auto constant = Array<Data>{
    Array<Int>{storage::ConstantStorage<std::int64_t>{1, std::int64_t{7}}}};
  auto narrow_values = storage::DataOwner<std::int8_t[]>::Builder{};
  narrow_values.emplace_back(std::int8_t{1});
  auto narrow = Array<Data>{
    Array<Int>{storage::SparseStorage<std::int8_t>{narrow_values.finish()}}};
  CHECK_EQUAL(id(primary), id(constant));
  CHECK_EQUAL(id(primary), id(narrow));
}

TEST("type ids are stable across batches and preserve record field order") {
  auto first = ArrayBuilder<Data>{};
  {
    auto row = first.record();
    row.field("a").data(std::int64_t{1});
    row.field("b").data(std::string_view{"x"});
  }
  auto second = ArrayBuilder<Data>{};
  {
    auto row = second.record();
    row.field("a").data(std::int64_t{9});
    row.field("b").data(std::string_view{"y"});
  }
  auto reversed = ArrayBuilder<Data>{};
  {
    auto row = reversed.record();
    row.field("b").data(std::string_view{"x"});
    row.field("a").data(std::int64_t{1});
  }
  auto first_array = first.finish();
  CHECK_EQUAL(id(first_array), id(second.finish()));
  CHECK_NOT_EQUAL(id(first_array), id(reversed.finish()));
}

TEST("type ids handle sparse masks, null, and heterogeneous nested lists") {
  auto builder = ArrayBuilder<Data>{};
  builder.skip();
  builder.null();
  {
    auto row = builder.list();
    row.data(std::int64_t{1});
    row.data(std::string_view{"x"});
    row.list().data(true);
  }
  {
    auto row = builder.list();
    row.list().data(false);
    row.data(std::string_view{"y"});
    row.data(std::int64_t{2});
  }
  auto array = builder.finish();
  auto mask = storage::BitMap::Mutable{array.length()};
  mask.set(1, true);
  mask.set(2, true);
  mask.set(3, true);
  auto ids = type_id(array, std::move(mask).finish());
  CHECK_NOT_EQUAL(*ids.get(1), *ids.get(2));
  CHECK_EQUAL(*ids.get(2), *ids.get(3));
}

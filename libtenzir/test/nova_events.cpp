//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/fwd.hpp"

#include "tenzir/nova/array_builder.hpp"
#include "tenzir/nova/events.hpp"
#include "tenzir/nova/slice.hpp"
#include "tenzir/test/test.hpp"

#include <caf/type_id.hpp>

#include <array>
#include <chrono>
#include <cstdint>
#include <string>

using namespace tenzir;
using namespace tenzir::nova;

TEST("mask slicing preserves physical rows, metadata, and sharing") {
  auto data = ArrayBuilder<Record>{};
  auto names = ArrayBuilder<String>{};
  for (auto i = 0; i < 4; ++i) {
    data.record().field("x").data(static_cast<Int>(i));
  }
  names.data("name-0");
  names.data("name-1");
  names.data("name-2");
  names.data("name-3");
  auto mask = storage::BitMap::Builder{};
  mask.emplace_back(true);
  mask.emplace_back(false);
  mask.emplace_back(true);
  mask.emplace_back(false);
  auto events
    = Events{data.finish(), mask.finish(),
             Events::Meta{names.finish(),
                          Array<Time>{storage::ConstantStorage<Time>{4, {}}},
                          Array<Bool>{storage::BitMap{4, false}}}};

  auto result = mask_slice(events, 1, 3);

  CHECK_EQUAL(result.length(), 4);
  CHECK_EQUAL(result.active_count(), 1);
  CHECK(not result.mask.get(0));
  CHECK(not result.mask.get(1));
  CHECK(result.mask.get(2));
  CHECK(not result.mask.get(3));
  CHECK_EQUAL(*result.meta.name.get(0), "name-0");
  CHECK_EQUAL(*result.meta.name.get(2), "name-2");
  CHECK_EQUAL(&*as<storage::RecordStorage>(result.data.storage()),
              &*as<storage::RecordStorage>(events.data.storage()));
  auto physical = physically_slice(events, 1, 3);
  CHECK_EQUAL(physical.length(), 1);
  CHECK_EQUAL(physical.active_count(), 1);
  CHECK_EQUAL(*physical.meta.name.get(0), "name-2");
  CHECK_EQUAL(*physical.data.field("x")->data.try_as<Int>()->get(0), 2);
}

TEST("default-constructed events are empty") {
  const auto events = Events{};
  CHECK_EQUAL(events.length(), 0);
  CHECK_EQUAL(events.active_count(), 0);
}

TEST("events have a CAF type id") {
  // Registration is what lets `nova::Events` appear in actor interfaces. It
  // carries no inspector yet, so such messages stay within one process.
  CHECK_NOT_EQUAL(caf::type_id_v<tenzir::nova::Events>, caf::invalid_type_id);
}

TEST("physical reduction preserves nested values, field order, and metadata") {
  auto data = ArrayBuilder<nova::Record>{};
  auto names = ArrayBuilder<String>{};
  auto times = ArrayBuilder<Time>{};
  auto internal = ArrayBuilder<Bool>{};
  auto mask = storage::BitMap::Builder{};
  for (auto i = 0; i < 5; ++i) {
    auto row = data.record();
    if (i % 2 == 0) {
      row.field("first").data(static_cast<Int>(i));
    }
    auto list = row.field("values").list();
    list.data(static_cast<Int>(i));
    list.record().field("nested").data("text");
    list.null();
    row.field("last").data(i % 2 == 0);
    names.data(i % 2 == 0 ? "even" : "odd");
    times.data(Time{} + std::chrono::seconds{i});
    internal.data(i % 2 == 0);
    mask.emplace_back(i == 1 or i == 4);
  }
  auto input = Events{data.finish(),
                      mask.finish(),
                      {names.finish(), times.finish(), internal.finish()}};
  auto result = physically_reduce_to_mask(input);
  CHECK_EQUAL(result.length(), 2);
  CHECK_EQUAL(result.active_count(), 2);
  auto source_rows = std::array{storage::Index{1}, storage::Index{4}};
  for (auto row = storage::Index{0}; row < result.length(); ++row) {
    auto source = source_rows[row];
    CHECK(equal(RowView<nova::Data>{result.data.get(row)},
                RowView<nova::Data>{input.data.get(source)}));
    CHECK_EQUAL(*result.meta.name.get(row), *input.meta.name.get(source));
    CHECK_EQUAL(*result.meta.import_time.get(row),
                *input.meta.import_time.get(source));
    CHECK_EQUAL(*result.meta.internal.get(row),
                *input.meta.internal.get(source));
  }
  CHECK_NOT_EQUAL(&*as<storage::RecordStorage>(result.data.storage()),
                  &*as<storage::RecordStorage>(input.data.storage()));
}

TEST("physical reduction handles constants and empty selections") {
  auto records = repeat(nova::Data{nova::Record{
                          {"list", nova::List{Int{1}, String{"value"}}}}},
                        4)
                   .try_as<nova::Record>();
  REQUIRE(records.is_some());
  auto input = Events{std::move(*records), storage::BitMap{4, true},
                      Events::Meta::make_empty(4, "constant")};
  auto result = physically_slice(input, 1, 3);
  CHECK_EQUAL(result.length(), 2);
  CHECK(equal(RowView<nova::Data>{result.data.get(0)},
              RowView<nova::Data>{input.data.get(1)}));
  CHECK_EQUAL(*result.meta.name.get(1), "constant");
  auto empty = physically_slice(input, 2, 2);
  CHECK_EQUAL(empty.length(), 0);
  CHECK_EQUAL(empty.active_count(), 0);
  CHECK_EQUAL(empty.meta.name.length(), 0);
  CHECK_EQUAL(empty.meta.import_time.length(), 0);
  CHECK_EQUAL(empty.meta.internal.length(), 0);
  auto zero = physically_reduce_to_mask(Events{});
  CHECK_EQUAL(zero.length(), 0);
}

TEST("physical reduction compacts overlapping list spans") {
  auto values = ArrayBuilder<Int>{};
  for (auto i = 0; i < 5; ++i) {
    values.data(i);
  }
  auto spans = storage::DataOwner<storage::Span[]>::Builder{};
  spans.emplace_back(storage::Span{1, 4});
  spans.emplace_back(storage::Span{0, 1});
  spans.emplace_back(storage::Span{2, 5});
  auto lists = Array<List>{spans.finish(), Array<nova::Data>{values.finish()}};
  auto records = Array<nova::Record>::make_empty(3).with_field_overwrite(
    "list", {Array<nova::Data>{lists}, storage::BitMap{3, true}});
  auto mask = storage::BitMap::Mutable{3};
  mask.set(0, true);
  mask.set(2, true);
  auto input = Events{std::move(records), std::move(mask).finish(),
                      Events::Meta::make_empty(3)};
  auto result = physically_reduce_to_mask(input);
  auto output = result.data.field("list")->data.try_as<List>();
  REQUIRE(output.is_some());
  CHECK_EQUAL(output->length(), 2);
  CHECK_EQUAL(as<storage::ListStorage>(output->storage()).values().length(), 4);
  CHECK(equal(RowView<nova::Data>{output->get(0)},
              RowView<nova::Data>{lists.get(0)}));
  CHECK(equal(RowView<nova::Data>{output->get(1)},
              RowView<nova::Data>{lists.get(2)}));
}

TEST("physical reduction releases excluded payloads and skips absent records") {
  auto data = ArrayBuilder<nova::Record>{};
  data.skip();
  data.record().field("payload").data(std::string(1024 * 1024, 'x'));
  data.record().field("keep").data(Int{42});
  auto mask = storage::BitMap::Mutable{3};
  mask.set(2, true);
  auto input = Events{data.finish(), std::move(mask).finish(),
                      Events::Meta::make_empty(3)};
  auto result = physically_reduce_to_mask(input);
  CHECK_EQUAL(result.length(), 1);
  CHECK_LESS(result.approx_bytes(), size_t{64 * 1024});
  CHECK(equal(RowView<nova::Data>{result.data.get(0)},
              RowView<nova::Data>{input.data.get(2)}));
}

TEST("mask slicing composes physical ranges without reactivating rows") {
  auto mask = storage::BitMap::Mutable{5};
  mask.set(1, true);
  mask.set(3, true);
  auto input = Events{Array<nova::Record>::make_empty(5),
                      std::move(mask).finish(), Events::Meta::make_empty(5)};
  auto result = mask_slice(mask_slice(input, 1, 4), 2, 5);
  CHECK_EQUAL(result.length(), 5);
  CHECK_EQUAL(result.active_count(), 1);
  CHECK(result.mask.get(3));
  auto empty = mask_slice(input, 2, 2);
  CHECK_EQUAL(empty.length(), 5);
  CHECK_EQUAL(empty.active_count(), 0);
  auto all = mask_slice(input, 0, input.length());
  CHECK_EQUAL(all.active_count(), input.active_count());
}

TEST("physical reduction preserves narrow physical columns") {
  auto values = storage::DataOwner<int16_t[]>::make_value(4, int16_t{7});
  auto column = Array<Int>{storage::SparseStorage<int16_t>{std::move(values)}};
  auto input
    = Events{Array<nova::Record>::make_empty(4).with_field_overwrite(
               "x", {Array<nova::Data>{column}, storage::BitMap{4, true}}),
             storage::BitMap{4, true}, Events::Meta::make_empty(4)};
  auto result = physically_slice(input, 1, 3);
  auto output = result.data.field("x")->data.try_as<Int>();
  REQUIRE(output.is_some());
  CHECK(is<storage::SparseStorage<int16_t>>(output->storage()));
  CHECK_EQUAL(*output->get(0), 7);
  CHECK_EQUAL(*output->get(1), 7);
}

TEST("physical reduction rebases unordered list spans across bitmap words") {
  auto values = ArrayBuilder<Int>{};
  for (auto i = 0; i < 256; ++i) {
    values.data(i);
  }
  auto spans = storage::DataOwner<storage::Span[]>::Builder{};
  spans.emplace_back(storage::Span{128, 134});
  spans.emplace_back(storage::Span{0, 2});
  spans.emplace_back(storage::Span{126, 130});
  spans.emplace_back(storage::Span{255, 256});
  auto lists = Array<List>{spans.finish(), Array<nova::Data>{values.finish()}};
  auto records = Array<nova::Record>::make_empty(4).with_field_overwrite(
    "list", {Array<nova::Data>{lists}, storage::BitMap{4, true}});
  auto mask = storage::BitMap::Mutable{4};
  mask.set(0, true);
  mask.set(2, true);
  mask.set(3, true);
  auto input = Events{std::move(records), std::move(mask).finish(),
                      Events::Meta::make_empty(4)};
  auto result = physically_reduce_to_mask(input);
  auto output = result.data.field("list")->data.try_as<List>();
  REQUIRE(output.is_some());
  CHECK_EQUAL(output->length(), 3);
  CHECK_EQUAL(as<storage::ListStorage>(output->storage()).values().length(), 9);
  auto source_rows
    = std::array{storage::Index{0}, storage::Index{2}, storage::Index{3}};
  for (auto row = storage::Index{0}; row < output->length(); ++row) {
    CHECK(equal(RowView<nova::Data>{output->get(row)},
                RowView<nova::Data>{lists.get(source_rows[row])}));
  }
}

TEST("physical reduction shares batches without inactive rows") {
  auto data = ArrayBuilder<nova::Record>{};
  data.record().field("x").data(Int{1});
  data.record().field("x").data(Int{2});
  auto input = Events{data.finish(), storage::BitMap{2, true},
                      Events::Meta::make_empty(2, "fully.active")};
  auto result = physically_reduce_to_mask(input);
  CHECK_EQUAL(result.length(), 2);
  CHECK_EQUAL(result.active_count(), 2);
  CHECK_EQUAL(&*as<storage::RecordStorage>(result.data.storage()),
              &*as<storage::RecordStorage>(input.data.storage()));
  CHECK_EQUAL((*result.meta.name.get(0)).data(),
              (*input.meta.name.get(0)).data());
}

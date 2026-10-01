//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/nova/materialize.hpp"
#include "tenzir/nova/sort.hpp"
#include "tenzir/test/test.hpp"

#include <caf/binary_deserializer.hpp>
#include <caf/binary_serializer.hpp>

using namespace tenzir;
using namespace tenzir::nova;

namespace {

auto add_batch(SortBuffer& buffer, std::vector<nova::Data> values,
               Int offset = 0, bool masked = false) -> void {
  auto events = ArrayBuilder<nova::Record>{};
  auto keys = ArrayBuilder<nova::Data>{};
  auto secondary = ArrayBuilder<nova::Data>{};
  auto mask
    = storage::BitMap::Mutable{detail::narrow<storage::Index>(values.size())};
  for (auto i = size_t{0}; i < values.size(); ++i) {
    auto row = events.record();
    row.field("id").data(offset + detail::narrow<Int>(i));
    // Alternating schemas and field order must survive compaction.
    if (i % 2 == 0) {
      row.field("extra").data("present");
    }
    append_row(row.field("key"), RowView<nova::Data>{values[i]});
    append_row(keys, RowView<nova::Data>{values[i]});
    secondary.data(detail::narrow<Int>(i % 3));
    mask.set(detail::narrow<storage::Index>(i), not masked or i % 3 != 0);
  }
  auto data = events.finish();
  auto length = data.length();
  buffer.add({std::move(data), std::move(mask).finish(),
              Events::Meta::make_empty(length, "sort.test")},
             {keys.finish(), secondary.finish()});
}

auto collect(SortBuffer& buffer, Option<uint64_t> limit = {})
  -> std::vector<tenzir::data> {
  auto result = std::vector<tenzir::data>{};
  for (auto batch : buffer.finish()) {
    storage::for_each_true(batch.mask, [&](storage::Index row) {
      if (not limit or result.size() < *limit) {
        result.push_back(
          materialize_legacy(RowView<nova::Data>{batch.data.get(row)}));
      }
      CHECK_EQUAL(*batch.meta.name.get(row), "sort.test");
      CHECK_EQUAL(*batch.meta.import_time.get(row), tenzir::time{});
      CHECK(not *batch.meta.internal.get(row));
    });
  }
  return result;
}

} // namespace

TEST("bounded sort equals a full stable sort across batches and masks") {
  auto values = std::vector<nova::Data>{
    Null{},
    Int{1},
    UInt{1},
    Float{1.0},
    Int{-1},
    Int{std::numeric_limits<Int>::min()},
    UInt{std::numeric_limits<UInt>::max()},
    Float{9007199254740992.0},
    Int{9007199254740993},
    String{"a"},
    nova::List{Int{2}, Null{}},
    nova::Record{{"a", Int{1}}},
    Null{},
  };
  for (auto order : {Order::ascending, Order::descending}) {
    for (auto masked : {false, true}) {
      for (auto limit : {uint64_t{0}, uint64_t{1}, uint64_t{3}, uint64_t{8},
                         uint64_t{100}, std::numeric_limits<uint64_t>::max()}) {
        auto full = SortBuffer{{order, Order::descending}, None{}};
        auto bounded = SortBuffer{{order, Order::descending}, limit};
        for (auto batch = Int{0}; batch < 4; ++batch) {
          add_batch(full, values, batch * 100, masked);
          add_batch(bounded, values, batch * 100, masked);
          CHECK_LESS_EQUAL(bounded.rows(), limit);
        }
        CHECK_EQUAL(collect(bounded), collect(full, limit));
      }
    }
  }
}

TEST("bounded sort releases rejected event data and evaluated key columns") {
  auto buffer = SortBuffer{{Order::ascending}, uint64_t{3}};
  for (auto batch = 0; batch < 24; ++batch) {
    auto events = ArrayBuilder<nova::Record>{};
    auto keys = ArrayBuilder<nova::Data>{};
    for (auto row = 0; row < 128; ++row) {
      auto value = std::to_string(batch * 128 + row) + std::string(4096, 'x');
      events.record().field("payload").data(value);
      keys.data(value);
    }
    auto data = events.finish();
    auto length = data.length();
    buffer.add({std::move(data), storage::BitMap{length, true},
                Events::Meta::make_empty(length)},
               {keys.finish()});
    CHECK_EQUAL(buffer.rows(), 3u);
    // One incoming batch owns over 1 MiB of data and keys. The retained
    // columns must instead own only three rows, independently of batch count.
    CHECK_LESS(buffer.approx_bytes(), size_t{64 * 1024});
  }
}

TEST("bounded sort copies input views even before reaching the bound") {
  auto events = ArrayBuilder<nova::Record>{};
  for (auto row = 0; row < 128; ++row) {
    events.record().field("payload").data(std::string(4096, 'x'));
  }
  auto data = events.finish();
  auto length = data.length();
  auto input = Events{std::move(data), storage::BitMap{length, true},
                      Events::Meta::make_empty(length)};
  auto buffer = SortBuffer{{Order::ascending}, uint64_t{3}};
  for (auto row = storage::Index{0}; row < 3; ++row) {
    auto view = subslice(input, row, row + 1);
    auto key = view.data.field("payload")->data;
    buffer.add(std::move(view), {std::move(key)});
    CHECK_EQUAL(buffer.rows(), detail::narrow<size_t>(row + 1));
    CHECK_LESS(buffer.approx_bytes(), size_t{64 * 1024});
  }
}

TEST("bounded sort preserves metadata when compacting candidates") {
  auto buffer = SortBuffer{{Order::ascending}, uint64_t{2}};
  auto events = ArrayBuilder<nova::Record>{};
  auto keys = ArrayBuilder<nova::Data>{};
  auto names = ArrayBuilder<String>{};
  auto times = ArrayBuilder<Time>{};
  auto internal = ArrayBuilder<Bool>{};
  for (auto key : {Int{2}, Int{0}, Int{1}, Int{3}, Int{4}}) {
    events.record().field("key").data(key);
    keys.data(key);
    names.data(std::to_string(key));
    times.data(tenzir::time{std::chrono::seconds{key}});
    internal.data(key == 0);
  }
  buffer.add({events.finish(),
              storage::BitMap{5, true},
              {names.finish(), times.finish(), internal.finish()}},
             {keys.finish()});
  auto row = Int{0};
  for (auto batch : buffer.finish()) {
    for (auto i = storage::Index{0}; i < batch.length(); ++i) {
      CHECK_EQUAL(*batch.meta.name.get(i), std::to_string(row));
      CHECK_EQUAL(*batch.meta.import_time.get(i),
                  tenzir::time{std::chrono::seconds{row}});
      CHECK_EQUAL(*batch.meta.internal.get(i), row == 0);
      ++row;
    }
  }
  CHECK_EQUAL(row, 2);
}

TEST("sort snapshots retain evaluated keys and stable heap tie order") {
  auto orders = std::vector<Order>{Order::ascending, Order::descending};
  for (auto limit :
       {Option<uint64_t>{}, Option<uint64_t>{0}, Option<uint64_t>{4}}) {
    auto original = SortBuffer{orders, limit};
    add_batch(original, {Int{1}, Int{1}, Null{}, Int{0}, UInt{1}}, 0, true);
    auto bytes = caf::byte_buffer{};
    auto serializer = caf::binary_serializer{bytes};
    REQUIRE(serializer.apply(original));
    auto restored = SortBuffer{orders, limit};
    auto deserializer = caf::binary_deserializer{bytes};
    REQUIRE(deserializer.apply(restored));
    CHECK_EQUAL(restored.rows(), original.rows());
    add_batch(original, {Int{1}, Int{-1}, Float{1.0}, Null{}}, 100);
    add_batch(restored, {Int{1}, Int{-1}, Float{1.0}, Null{}}, 100);
    CHECK_EQUAL(collect(restored), collect(original));
  }
}

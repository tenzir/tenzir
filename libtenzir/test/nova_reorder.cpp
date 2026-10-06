//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/detail/event_time_reorder_buffer.hpp"
#include "tenzir/nova/materialize.hpp"
#include "tenzir/nova/reorder.hpp"
#include "tenzir/test/test.hpp"

#include <caf/binary_deserializer.hpp>
#include <caf/binary_serializer.hpp>

#include <chrono>
#include <random>
#include <string>
#include <vector>

using namespace tenzir;
using namespace tenzir::nova;
using namespace std::chrono_literals;

namespace {

struct Row {
  int64_t id;
  tenzir::time timestamp;
  bool active = true;
  bool valid = true;
};

auto add(ReorderBuffer& buffer, std::vector<Row> const& rows,
         size_t padding = 0) -> ReorderBuffer::Counts {
  auto events = ArrayBuilder<nova::Record>{};
  auto times = ArrayBuilder<Time>{};
  auto names = ArrayBuilder<String>{};
  auto import_times = ArrayBuilder<Time>{};
  auto internal = ArrayBuilder<Bool>{};
  auto active = storage::BitMap::Builder{};
  auto valid = storage::BitMap::Builder{};
  for (auto const& row : rows) {
    auto record = events.record();
    record.field("id").data(row.id);
    // Different shapes and nested values must survive gathering and compaction.
    if (row.id % 2 == 0) {
      record.field("extra").data("present");
    }
    auto nested = record.field("nested").list();
    nested.data(row.id);
    nested.data("payload");
    if (padding > 0) {
      record.field("padding").data(std::string(padding, 'x'));
    }
    times.data(row.timestamp);
    names.data(std::to_string(row.id));
    import_times.data(tenzir::time{std::chrono::seconds{row.id}});
    internal.data(row.id % 2 == 0);
    active.emplace_back(row.active);
    valid.emplace_back(row.valid);
  }
  return buffer.add(
    {events.finish(),
     active.finish(),
     {names.finish(), import_times.finish(), internal.finish()}},
    MaskedArray<Array<Time>>{times.finish(), valid.finish()});
}

auto collect(generator<Events> output) -> std::vector<int64_t> {
  auto result = std::vector<int64_t>{};
  for (auto const& events : output) {
    CHECK_EQUAL(events.active_count(), events.length());
    CHECK_LESS_EQUAL(detail::narrow<uint64_t>(events.length()),
                     defaults::import::table_slice_size);
    auto ids = events.data.field("id")->data.get_alternative<Int>();
    TENZIR_ASSERT(ids);
    for (auto i = storage::Index{0}; i < events.length(); ++i) {
      auto id = *ids->data.get(i);
      result.push_back(id);
      CHECK_EQUAL(*events.meta.name.get(i), std::to_string(id));
      CHECK_EQUAL(*events.meta.import_time.get(i),
                  tenzir::time{std::chrono::seconds{id}});
      CHECK_EQUAL(*events.meta.internal.get(i), id % 2 == 0);
      CHECK_EQUAL(materialize_legacy(events.data.field("nested")->data.get(i)),
                  (tenzir::data{tenzir::list{id, "payload"}}));
      auto extra = events.data.field("extra");
      CHECK_EQUAL(extra and extra->present.get(i), id % 2 == 0);
    }
  }
  return result;
}

} // namespace

TEST("reorder heap advances the watermark per row and keeps stable ties") {
  auto buffer = ReorderBuffer{1s};
  auto counts = add(buffer, {{2, tenzir::time{2s}},
                             {1, tenzir::time{1s}},
                             {3, tenzir::time{2s}}});
  CHECK_EQUAL(counts.invalid, 0);
  CHECK_EQUAL(counts.late, 0);
  CHECK_EQUAL(collect(buffer.drain()), (std::vector<int64_t>{1}));
  CHECK_EQUAL(buffer.rows(), 2u);
  CHECK_EQUAL(collect(buffer.flush()), (std::vector<int64_t>{2, 3}));
  CHECK_EQUAL(buffer.batches(), 0u);
  CHECK_EQUAL(buffer.physical_rows(), 0u);
  counts = add(buffer, {{4, tenzir::time{1s}}});
  CHECK_EQUAL(counts.late, 1);
  CHECK(collect(buffer.drain()).empty());
}

TEST("reorder rejects intra-batch regressions but accepts emitted timestamp "
     "ties") {
  auto buffer = ReorderBuffer{0s};
  auto counts = add(buffer, {{2, tenzir::time{2s}},
                             {1, tenzir::time{1s}},
                             {3, tenzir::time{2s}}});
  CHECK_EQUAL(counts.late, 1);
  CHECK_EQUAL(collect(buffer.drain()), (std::vector<int64_t>{2, 3}));
  CHECK_EQUAL(buffer.rows(), 0u);
  CHECK_EQUAL(buffer.batches(), 0u);
}

TEST("reorder ignores inactive timestamps and counts only active invalid "
     "rows") {
  auto buffer = ReorderBuffer{1s};
  auto counts = add(buffer, {{99, tenzir::time{100s}, false},
                             {98, tenzir::time{0s}, true, false},
                             {2, tenzir::time{2s}},
                             {1, tenzir::time{1s}}});
  CHECK_EQUAL(counts.invalid, 1);
  CHECK_EQUAL(counts.late, 0);
  CHECK_EQUAL(collect(buffer.drain()), (std::vector<int64_t>{1}));
  CHECK_EQUAL(buffer.physical_rows(), 1u);
  CHECK_EQUAL(collect(buffer.flush()), (std::vector<int64_t>{2}));
  auto events = ArrayBuilder<nova::Record>{};
  events.record().field("id").data(int64_t{0});
  counts = buffer.add({events.finish(), storage::BitMap{1, true},
                       Events::Meta::make_empty(1)},
                      None{});
  CHECK_EQUAL(counts.invalid, 1);
  CHECK(collect(buffer.drain()).empty());
  CHECK_EQUAL(buffer.batches(), 0u);
}

TEST("stalled event time retains a whole batch instead of allocating row "
     "batches") {
  auto buffer = ReorderBuffer{1h};
  auto rows = std::vector<Row>{};
  for (auto i = int64_t{0}; i < 100'001; ++i) {
    rows.push_back({i, tenzir::time{1s}});
  }
  auto counts = add(buffer, rows);
  CHECK_EQUAL(counts.max_retained, rows.size());
  CHECK(collect(buffer.drain()).empty());
  CHECK_EQUAL(buffer.rows(), rows.size());
  CHECK_EQUAL(buffer.physical_rows(), rows.size());
  CHECK_EQUAL(buffer.batches(), 1u);
  auto emitted = size_t{0};
  auto batches = size_t{0};
  for (auto events : buffer.flush()) {
    CHECK_LESS_EQUAL(detail::narrow<uint64_t>(events.length()),
                     defaults::import::table_slice_size);
    auto ids = events.data.field("id")->data.get_alternative<Int>();
    TENZIR_ASSERT(ids);
    CHECK_EQUAL(*ids->data.get(0), detail::narrow<int64_t>(emitted));
    emitted += detail::narrow<size_t>(events.length());
    CHECK_EQUAL(*ids->data.get(events.length() - 1),
                detail::narrow<int64_t>(emitted - 1));
    ++batches;
  }
  CHECK_EQUAL(emitted, rows.size());
  CHECK_EQUAL(batches, 13u);
  CHECK_EQUAL(buffer.physical_rows(), 0u);
}

TEST("reorder compacts sparse wide backing data without losing metadata or "
     "ties") {
  auto buffer = ReorderBuffer{1h};
  auto rows = std::vector<Row>{};
  for (auto i = int64_t{0}; i < 1024; ++i) {
    rows.push_back({i, tenzir::time{1s}, i == 5 or i == 6});
  }
  add(buffer, rows, 4096);
  CHECK_EQUAL(buffer.physical_rows(), 1024u);
  CHECK(collect(buffer.drain()).empty());
  CHECK_EQUAL(buffer.rows(), 2u);
  CHECK_EQUAL(buffer.physical_rows(), 2u);
  CHECK_EQUAL(buffer.batches(), 1u);
  CHECK_LESS(buffer.approx_bytes(), size_t{64 * 1024});
  add(buffer, {{7, tenzir::time{1s}}});
  CHECK(collect(buffer.drain()).empty());
  CHECK_EQUAL(collect(buffer.flush()), (std::vector<int64_t>{5, 6, 7}));
}

TEST("reorder releases drained batches and compacts a small surviving tail") {
  auto buffer = ReorderBuffer{1s};
  auto rows = std::vector<Row>{};
  auto expected = std::vector<int64_t>{};
  for (auto i = int64_t{0}; i < 4096; ++i) {
    rows.push_back({i, tenzir::time{std::chrono::seconds{i}}});
    if (i < 4095) {
      expected.push_back(i);
    }
  }
  add(buffer, rows);
  CHECK_EQUAL(collect(buffer.drain()), expected);
  CHECK_EQUAL(buffer.rows(), 1u);
  CHECK_EQUAL(buffer.physical_rows(), 1u);
  CHECK_EQUAL(buffer.batches(), 1u);
  CHECK_EQUAL(collect(buffer.flush()), (std::vector<int64_t>{4095}));
}

TEST("reorder compaction gathers survivors into multiple bounded batches") {
  auto buffer = ReorderBuffer{1h};
  auto rows = std::vector<Row>{};
  auto expected = std::vector<int64_t>{};
  for (auto i = int64_t{0}; i < 20'000; ++i) {
    auto active = i < 8193;
    rows.push_back({i, tenzir::time{1s}, active});
    if (active) {
      expected.push_back(i);
    }
  }
  add(buffer, rows);
  CHECK(collect(buffer.drain()).empty());
  CHECK_EQUAL(buffer.physical_rows(), expected.size());
  CHECK_EQUAL(buffer.batches(), 2u);
  auto bytes = caf::byte_buffer{};
  auto serializer = caf::binary_serializer{bytes};
  REQUIRE(serializer.apply(buffer));
  auto restored = ReorderBuffer{1h};
  auto deserializer = caf::binary_deserializer{bytes};
  REQUIRE(deserializer.apply(restored));
  CHECK_EQUAL(collect(restored.flush()), expected);
  CHECK_EQUAL(collect(buffer.flush()), expected);
}

TEST("reorder snapshots preserve ready rows watermarks and stable tie "
     "sequences") {
  auto original = ReorderBuffer{5s};
  add(original, {{10, tenzir::time{10s}}, {4, tenzir::time{4s}}});
  auto bytes = caf::byte_buffer{};
  auto serializer = caf::binary_serializer{bytes};
  REQUIRE(serializer.apply(original));
  auto restored = ReorderBuffer{5s};
  auto deserializer = caf::binary_deserializer{bytes};
  REQUIRE(deserializer.apply(restored));
  CHECK_EQUAL(collect(original.drain()), collect(restored.drain()));
  for (auto* buffer : {&original, &restored}) {
    auto counts = add(*buffer, {{3, tenzir::time{3s}},
                                {5, tenzir::time{4s}},
                                {11, tenzir::time{10s}}});
    CHECK_EQUAL(counts.late, 1);
    CHECK_EQUAL(collect(buffer->drain()), (std::vector<int64_t>{5}));
  }
  CHECK_EQUAL(collect(original.flush()), (std::vector<int64_t>{10, 11}));
  CHECK_EQUAL(collect(restored.flush()), (std::vector<int64_t>{10, 11}));
}

TEST("reorder saturates watermark subtraction at the minimum timestamp") {
  auto buffer = ReorderBuffer{10s};
  add(buffer, {{0, tenzir::time::min()}});
  CHECK_EQUAL(collect(buffer.drain()), (std::vector<int64_t>{0}));
  CHECK_EQUAL(buffer.rows(), 0u);
}

TEST("reorder heap agrees with incremental event-time ordering across "
     "batches") {
  using Reference = detail::EventTimeReorderBuffer<int64_t>;
  for (auto tolerance : {0s, 1s, 10s, 100s}) {
    auto buffer = ReorderBuffer{tolerance};
    auto reference = Reference{tolerance};
    auto random = std::mt19937{42};
    for (auto batch = int64_t{0}; batch < 100; ++batch) {
      auto rows = std::vector<Row>{};
      auto expected = std::vector<int64_t>{};
      auto invalid = int64_t{0};
      auto late = int64_t{0};
      for (auto i = int64_t{0}; i < 31; ++i) {
        auto row = Row{batch * 31 + i,
                       tenzir::time{std::chrono::seconds{random() % 100}},
                       random() % 4 != 0, random() % 7 != 0};
        rows.push_back(row);
        if (not row.active) {
          continue;
        }
        if (not row.valid) {
          ++invalid;
          continue;
        }
        if (reference.insert(row.timestamp, row.id)
            == Reference::InsertResult::late) {
          ++late;
          continue;
        }
        for (auto const& event : reference.drain()) {
          expected.push_back(event.payload);
        }
      }
      auto counts = add(buffer, rows);
      CHECK_EQUAL(counts.invalid, invalid);
      CHECK_EQUAL(counts.late, late);
      CHECK_EQUAL(collect(buffer.drain()), expected);
      CHECK_LESS_EQUAL(buffer.physical_rows(), 2 * buffer.rows());
    }
    auto expected = std::vector<int64_t>{};
    for (auto const& event : reference.flush()) {
      expected.push_back(event.payload);
    }
    CHECK_EQUAL(collect(buffer.flush()), expected);
  }
}

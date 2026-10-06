//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/defaults.hpp"
#include "tenzir/detail/narrow.hpp"
#include "tenzir/detail/saturating_arithmetic.hpp"
#include "tenzir/generator.hpp"
#include "tenzir/nova/array_builder.hpp"
#include "tenzir/nova/bitmap_iteration.hpp"
#include "tenzir/nova/events.hpp"
#include "tenzir/nova/union_array.hpp"
#include "tenzir/time.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <utility>
#include <vector>

namespace tenzir::nova {

/// Incrementally orders active rows by event time, retaining whole input
/// batches and a stable min-heap of row indices. Rows are copied only when
/// emitting them or compacting backing batches that contain mostly
/// discarded/emitted rows.
class ReorderBuffer {
public:
  struct Counts {
    int64_t invalid = 0;
    int64_t late = 0;
    size_t max_retained = 0;
  };

  explicit ReorderBuffer(duration tolerance) : tolerance_{tolerance} {
    TENZIR_ASSERT(tolerance >= duration::zero());
  }

  /// Evaluate timestamps once before calling this. The watermark advances in
  /// arrival order, including within a batch, so later rows can still be late.
  auto add(Events events, Option<MaskedArray<Array<Time>>> timestamps)
    -> Counts {
    TENZIR_ASSERT(ready_.empty());
    auto counts = Counts{.max_retained = heap_.size()};
    if (not timestamps) {
      counts.invalid = events.active_count();
      return counts;
    }
    TENZIR_ASSERT(timestamps->data.length() == events.length());
    auto batch = size_t{0};
    if (free_batches_.empty()) {
      batch = batches_.size();
      batches_.push_back({std::move(events), 0});
    } else {
      batch = free_batches_.back();
      free_batches_.pop_back();
      batches_[batch] = {std::move(events), 0};
    }
    physical_rows_ += detail::narrow<size_t>(batches_[batch].events.length());
    storage::for_each_true(batches_[batch].events.mask, [&](auto row) {
      if (not timestamps->present.get(row)) {
        ++counts.invalid;
        return;
      }
      auto timestamp = *timestamps->data.get(row);
      if (last_emitted_ and timestamp < *last_emitted_) {
        ++counts.late;
        return;
      }
      if (next_sequence_ == std::numeric_limits<uint64_t>::max()) {
        resequence();
      }
      largest_observed_ = largest_observed_
                            ? std::max(*largest_observed_, timestamp)
                            : timestamp;
      heap_.push_back({batch, row, timestamp, next_sequence_++});
      ++batches_[batch].references;
      std::push_heap(heap_.begin(), heap_.end(), Later{});
      auto watermark = detail::saturating_sub(*largest_observed_, tolerance_);
      while (not heap_.empty() and heap_.front().timestamp <= watermark) {
        make_ready();
      }
      counts.max_retained = std::max(counts.max_retained, heap_.size());
    });
    if (batches_[batch].references == 0) {
      release_batch(batch);
    }
    return counts;
  }

  /// Gathers ready rows directly from their batches into bounded output batches.
  auto drain() -> generator<Events> {
    auto builder = EventsBuilder{};
    for (auto const& index : ready_) {
      auto& batch = batches_[index.batch];
      builder.append(batch.events, index.row);
      TENZIR_ASSERT(batch.references > 0);
      if (--batch.references == 0) {
        release_batch(index.batch);
      }
      if (builder.data.length() == batch_size) {
        co_yield std::exchange(builder, EventsBuilder{}).finish();
      }
    }
    ready_.clear();
    if (builder.data.length() > 0) {
      co_yield std::move(builder).finish();
    }
    // Reclaim sparse backing batches in bulk. Amortize the O(buffered rows)
    // copy over at least that many inactive, rejected, or emitted physical rows.
    if (heap_.empty()) {
      batches_.clear();
      free_batches_.clear();
      TENZIR_ASSERT(physical_rows_ == 0);
    } else if (physical_rows_ - heap_.size() > heap_.size()) {
      compact();
    }
  }

  auto flush() -> generator<Events> {
    while (not heap_.empty()) {
      make_ready();
    }
    for (auto events : drain()) {
      co_yield std::move(events);
    }
  }

  auto rows() const -> size_t {
    return heap_.size();
  }

  auto physical_rows() const -> size_t {
    return physical_rows_;
  }

  auto batches() const -> size_t {
    return batches_.size() - free_batches_.size();
  }

  auto approx_bytes() const -> size_t {
    auto result = batches_.capacity() * sizeof(Batch)
                  + free_batches_.capacity() * sizeof(size_t)
                  + (heap_.capacity() + ready_.capacity()) * sizeof(Index);
    for (auto const& batch : batches_) {
      result += batch.events.approx_bytes();
    }
    return result;
  }

  friend auto inspect(auto& f, ReorderBuffer& x) -> bool {
    // Tolerance is an immutable operator argument. Index timestamps must be
    // restored directly rather than reevaluating a possibly nondeterministic key.
    return f.object(x).fields(f.field("batches", x.batches_),
                              f.field("free_batches", x.free_batches_),
                              f.field("heap", x.heap_),
                              f.field("ready", x.ready_),
                              f.field("physical_rows", x.physical_rows_),
                              f.field("largest_observed", x.largest_observed_),
                              f.field("last_emitted", x.last_emitted_),
                              f.field("next_sequence", x.next_sequence_));
  }

private:
  static constexpr auto batch_size
    = detail::narrow_cast<storage::Index>(defaults::import::table_slice_size);

  struct Batch {
    Events events;
    size_t references = 0;

    friend auto inspect(auto& f, Batch& x) -> bool {
      return f.object(x).fields(f.field("events", x.events),
                                f.field("references", x.references));
    }
  };

  struct Index {
    size_t batch;
    storage::Index row;
    time timestamp;
    uint64_t sequence;

    friend auto inspect(auto& f, Index& x) -> bool {
      return f.object(x).fields(f.field("batch", x.batch),
                                f.field("row", x.row),
                                f.field("timestamp", x.timestamp),
                                f.field("sequence", x.sequence));
    }
  };

  struct Later {
    auto operator()(Index const& lhs, Index const& rhs) const -> bool {
      return lhs.timestamp > rhs.timestamp
             or (lhs.timestamp == rhs.timestamp
                 and lhs.sequence > rhs.sequence);
    }
  };

  struct EventsBuilder {
    auto append(Events const& input, storage::Index row) -> void {
      TENZIR_ASSERT(input.mask.get(row));
      auto output = data.record();
      for (auto [name, value] : input.data.get(row)) {
        append_row(output.field(name), value);
      }
      names.data(*input.meta.name.get(row));
      import_times.data(*input.meta.import_time.get(row));
      internal.data(*input.meta.internal.get(row));
    }

    auto finish() && -> Events {
      auto records = data.finish();
      auto mask = storage::BitMap{records.length(), true};
      return {std::move(records),
              std::move(mask),
              {names.finish(), import_times.finish(), internal.finish()}};
    }

    ArrayBuilder<Record> data;
    ArrayBuilder<String> names;
    ArrayBuilder<Time> import_times;
    ArrayBuilder<Bool> internal;
  };

  auto make_ready() -> void {
    std::pop_heap(heap_.begin(), heap_.end(), Later{});
    last_emitted_ = heap_.back().timestamp;
    ready_.push_back(heap_.back());
    heap_.pop_back();
  }

  auto release_batch(size_t batch) -> void {
    TENZIR_ASSERT(batches_[batch].references == 0);
    physical_rows_ -= detail::narrow<size_t>(batches_[batch].events.length());
    batches_[batch].events = Events{};
    free_batches_.push_back(batch);
  }

  auto compact() -> void {
    TENZIR_ASSERT(ready_.empty());
    auto batches = std::vector<Batch>{};
    auto builder = EventsBuilder{};
    for (auto& index : heap_) {
      builder.append(batches_[index.batch].events, index.row);
      index.batch = batches.size();
      index.row = builder.data.length() - 1;
      if (builder.data.length() == batch_size) {
        batches.push_back({std::exchange(builder, EventsBuilder{}).finish(),
                           detail::narrow<size_t>(batch_size)});
      }
    }
    if (builder.data.length() > 0) {
      auto count = detail::narrow<size_t>(builder.data.length());
      batches.push_back({std::move(builder).finish(), count});
    }
    batches_ = std::move(batches);
    free_batches_.clear();
    physical_rows_ = heap_.size();
    // Only physical locations changed; timestamps, tie sequences, and heap
    // positions remain intact, so compaction does not require sorting again.
  }

  auto resequence() -> void {
    std::ranges::sort(heap_, [](Index const& lhs, Index const& rhs) {
      return Later{}(rhs, lhs);
    });
    next_sequence_ = 0;
    for (auto& index : heap_) {
      index.sequence = next_sequence_++;
    }
    std::make_heap(heap_.begin(), heap_.end(), Later{});
  }

  duration tolerance_;
  std::vector<Batch> batches_;
  std::vector<size_t> free_batches_;
  std::vector<Index> heap_;
  std::vector<Index> ready_;
  size_t physical_rows_ = 0;
  Option<time> largest_observed_;
  Option<time> last_emitted_;
  uint64_t next_sequence_ = 0;
};

} // namespace tenzir::nova

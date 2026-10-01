//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/defaults.hpp"
#include "tenzir/generator.hpp"
#include "tenzir/nova/array_builder.hpp"
#include "tenzir/nova/bitmap_iteration.hpp"
#include "tenzir/nova/comparison.hpp"
#include "tenzir/nova/events.hpp"

#include <algorithm>
#include <cstdint>
#include <span>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

namespace tenzir::nova {

/// Buffers a stable sort, or just its best `limit` rows. A bounded buffer owns
/// at most twice the bound in physical event/key rows between input batches.
class SortBuffer {
public:
  SortBuffer() = default;

  SortBuffer(std::vector<Order> orders, Option<uint64_t> limit)
    : orders_{std::move(orders)}, limit_{limit} {
  }

  auto add(Events events, std::vector<Array<Data>> keys) -> void {
    TENZIR_ASSERT(keys.size() == orders_.size());
    if (not events.mask.any() or (limit_ and *limit_ == 0)) {
      return;
    }
    for (auto const& key : keys) {
      TENZIR_ASSERT(key.length() == events.length());
    }
    if (not limit_) {
      retained_rows_ += detail::narrow<uint64_t>(events.length());
    }
    auto const batch = batches_.size();
    batches_.push_back({std::move(events), std::move(keys)});
    auto less = [this](size_t lhs, size_t rhs) {
      return compare(candidates_[lhs], candidates_[rhs]);
    };
    auto incoming = std::vector<size_t>{};
    storage::for_each_true(
      batches_.back().events.mask, [&](storage::Index row) {
        auto index = Index{batch, row, sequence_++};
        if (not limit_ or indices_.size() < *limit_) {
          auto slot = candidates_.size();
          candidates_.push_back(index);
          indices_.push_back(slot);
          if (limit_) {
            incoming.push_back(slot);
            std::push_heap(indices_.begin(), indices_.end(), less);
          }
        } else if (compare(index, candidates_[indices_.front()])) {
          std::pop_heap(indices_.begin(), indices_.end(), less);
          auto slot = indices_.back();
          candidates_[slot] = index;
          incoming.push_back(slot);
          std::push_heap(indices_.begin(), indices_.end(), less);
        }
      });
    if (limit_) {
      // Copy only incoming winners, then release the original batch. Even a
      // one-row input may be a view backed by an arbitrarily large allocation.
      std::ranges::sort(incoming);
      incoming.erase(std::unique(incoming.begin(), incoming.end()),
                     incoming.end());
      compact(incoming, batch);
      // Amortize global compaction over newly retained candidates. Subtraction
      // avoids overflowing for bounds near UINT64_MAX.
      if (retained_rows_ > *limit_ and retained_rows_ - *limit_ > *limit_) {
        compact(indices_);
      }
    }
  }

  /// Releases the buffered data in sorted, size-bounded output batches.
  auto finish() -> generator<Events> {
    std::ranges::sort(indices_, [this](size_t lhs, size_t rhs) {
      return compare(candidates_[lhs], candidates_[rhs]);
    });
    auto builder = EventsBuilder{};
    for (auto slot : indices_) {
      auto const& index = candidates_[slot];
      builder.append(batches_[index.batch].events, index.row);
      if (builder.data.length()
          == detail::narrow<storage::Index>(
            defaults::import::table_slice_size)) {
        co_yield std::exchange(builder, EventsBuilder{}).finish();
      }
    }
    if (builder.data.length() > 0) {
      co_yield std::move(builder).finish();
    }
    batches_.clear();
    indices_.clear();
    candidates_.clear();
    retained_rows_ = 0;
  }

  auto rows() const -> size_t {
    return indices_.size();
  }

  /// Includes physical data and key storage, not just the candidate index.
  auto approx_bytes() const -> size_t {
    auto result = indices_.capacity() * sizeof(size_t)
                  + candidates_.capacity() * sizeof(Index);
    for (auto const& batch : batches_) {
      result += batch.events.approx_bytes();
      for (auto const& key : batch.keys) {
        result += key.approx_bytes();
      }
    }
    return result;
  }

  friend auto inspect(auto& f, SortBuffer& x) -> bool {
    // Orders and the bound are immutable operator arguments.
    return f.object(x).fields(f.field("batches", x.batches_),
                              f.field("indices", x.indices_),
                              f.field("candidates", x.candidates_),
                              f.field("sequence", x.sequence_),
                              f.field("retained_rows", x.retained_rows_));
  }

private:
  struct EventsBuilder {
    auto append(Events const& input, storage::Index row) -> void {
      auto output = data.record();
      for (auto [name, value] : input.data.get(row)) {
        append_row(output.field(name), value);
      }
      names.data(*input.meta.name.get(row));
      import_times.data(*input.meta.import_time.get(row));
      internal.data(*input.meta.internal.get(row));
    }

    auto finish() && -> Events {
      auto result = data.finish();
      auto mask = storage::BitMap{result.length(), true};
      return {std::move(result),
              std::move(mask),
              {names.finish(), import_times.finish(), internal.finish()}};
    }

    ArrayBuilder<Record> data;
    ArrayBuilder<String> names;
    ArrayBuilder<Time> import_times;
    ArrayBuilder<Bool> internal;
  };

  struct Batch {
    Events events;
    std::vector<Array<Data>> keys;

    friend auto inspect(auto& f, Batch& x) -> bool {
      // Reuse event transport for the evaluated key columns. Restoring keys
      // must not reevaluate expressions, which may be nondeterministic.
      auto keys = Events{};
      auto count = x.keys.size();
      if constexpr (not std::remove_reference_t<decltype(f)>::is_loading) {
        auto names = std::vector<std::string>{};
        for (auto i = size_t{0}; i < x.keys.size(); ++i) {
          names.push_back(std::to_string(i));
        }
        auto fields = std::vector<
          std::pair<std::string_view, Array<Record>::MaskedArray>>{};
        for (auto i = size_t{0}; i < x.keys.size(); ++i) {
          fields.emplace_back(
            names[i], Array<Record>::MaskedArray{x.keys[i], x.events.mask});
        }
        keys = {Array<Record>::from_fields(fields), x.events.mask,
                Events::Meta::make_empty(x.events.length())};
      }
      if (not f.object(x).fields(f.field("events", x.events),
                                 f.field("keys", keys),
                                 f.field("key_count", count))) {
        return false;
      }
      if constexpr (std::remove_reference_t<decltype(f)>::is_loading) {
        x.keys.clear();
        for (auto i = size_t{0}; i < count; ++i) {
          x.keys.push_back(keys.data.field(std::to_string(i))->data);
        }
      }
      return true;
    }
  };

  struct Index {
    size_t batch;
    storage::Index row;
    uint64_t sequence;

    friend auto inspect(auto& f, Index& x) -> bool {
      return f.object(x).fields(f.field("batch", x.batch),
                                f.field("row", x.row),
                                f.field("sequence", x.sequence));
    }
  };

  auto compare(Index const& lhs, Index const& rhs) const -> bool {
    for (auto i = size_t{0}; i < orders_.size(); ++i) {
      auto relation
        = weak_order(batches_[lhs.batch].keys[i].get(lhs.row),
                     batches_[rhs.batch].keys[i].get(rhs.row), orders_[i]);
      if (relation != std::weak_ordering::equivalent) {
        return relation == std::weak_ordering::less;
      }
    }
    return lhs.sequence < rhs.sequence;
  }

  auto compact(std::span<size_t const> slots, Option<size_t> input_batch = {})
    -> void {
    auto events = EventsBuilder{};
    auto keys = std::vector<ArrayBuilder<Data>>(orders_.size());
    auto row = storage::Index{0};
    // Preserve heap positions and arrival sequence numbers while rewriting
    // their physical locations. Copying rows drops all rejected backing data.
    for (auto slot : slots) {
      auto& index = candidates_[slot];
      auto const& batch = batches_[index.batch];
      events.append(batch.events, index.row);
      for (auto i = size_t{0}; i < keys.size(); ++i) {
        append_row(keys[i], batch.keys[i].get(index.row));
      }
      index.batch = input_batch.unwrap_or(0);
      index.row = row++;
    }
    if (row == 0) {
      if (input_batch) {
        TENZIR_ASSERT(*input_batch + 1 == batches_.size());
        batches_.pop_back();
      } else {
        batches_.clear();
        retained_rows_ = 0;
      }
      return;
    }
    auto columns = std::vector<Array<Data>>{};
    for (auto& key : keys) {
      columns.push_back(key.finish());
    }
    auto result = Batch{std::move(events).finish(), std::move(columns)};
    if (input_batch) {
      batches_[*input_batch] = std::move(result);
      retained_rows_ += detail::narrow<uint64_t>(row);
    } else {
      batches_.clear();
      batches_.push_back(std::move(result));
      retained_rows_ = indices_.size();
    }
  }

  std::vector<Order> orders_;
  Option<uint64_t> limit_;
  std::vector<Batch> batches_;
  std::vector<size_t> indices_;
  std::vector<Index> candidates_;
  uint64_t sequence_ = 0;
  uint64_t retained_rows_ = 0;
};

} // namespace tenzir::nova

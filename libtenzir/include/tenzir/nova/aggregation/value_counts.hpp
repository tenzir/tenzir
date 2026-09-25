//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/nova/aggregation.hpp>
#include <tenzir/nova/data_array_builder.hpp>
#include <tenzir/nova/union_array.hpp>

#include <tsl/robin_map.h>

#include <cstdint>
#include <span>
#include <vector>

namespace tenzir::plugins::nova_value_counts {

struct ValueArgs {
  nova::ValueArgument x;
};

/// Whether a row is an explicit `null`.
inline auto is_null(nova::RowView<nova::Data> const& value) -> bool {
  return match(value, []<class Tag>(nova::RowView<Tag> const&) {
    return std::same_as<Tag, nova::Null>;
  });
}

/// Hashes and compares values like `nova::equivalent`, so borrowed rows can
/// be looked up without materializing them.
struct ValueHash {
  using is_transparent = void;

  auto operator()(nova::Data const& value) const noexcept -> size_t {
    return nova::hash(nova::RowView<nova::Data>{value});
  }

  auto operator()(nova::RowView<nova::Data> const& value) const noexcept
    -> size_t {
    return nova::hash(value);
  }
};

struct ValueEqual {
  using is_transparent = void;

  template <class Lhs, class Rhs>
  auto operator()(Lhs const& lhs, Rhs const& rhs) const -> bool {
    return nova::equivalent(nova::RowView<nova::Data>{lhs},
                            nova::RowView<nova::Data>{rhs});
  }
};

/// The distinct non-null values of an aggregation with a count each, in
/// first-seen order. Values are equal if `nova::equivalent` says so, which,
/// like `==`, does not distinguish numbers of different types, and unlike
/// `==` counts all NaNs as one value.
class ValueCounts {
public:
  auto add(nova::RowView<nova::Data> const& value) -> void {
    if (is_null(value)) {
      return;
    }
    auto it = positions_.find(value);
    if (it == positions_.end()) {
      positions_.emplace(nova::to_data(value), counts_.size());
      counts_.push_back(1);
      return;
    }
    ++counts_[it->second];
  }

  /// Folds the values of `x` at `rows`, in order.
  auto add(nova::Array<nova::Data> const& x, nova::storage::BitMap const& rows)
    -> void {
    for (auto row : nova::storage::true_bits(rows)) {
      add(x.get(row));
    }
  }

  auto add(nova::ListElements const& elements) -> void {
    for (auto i = elements.begin; i < elements.end; ++i) {
      add(elements.values.get(i));
    }
  }

  auto size() const -> size_t {
    return counts_.size();
  }

  auto counts() const -> std::span<int64_t const> {
    return counts_;
  }

  /// The values in first-seen order, parallel to `counts()`.
  auto values() const -> std::vector<nova::Data const*> {
    auto result = std::vector<nova::Data const*>(counts_.size());
    for (auto const& [value, position] : positions_) {
      result[position] = &value;
    }
    return result;
  }

private:
  /// Maps every value to its first-seen position.
  tsl::robin_map<nova::Data, size_t, ValueHash, ValueEqual> positions_;
  std::vector<int64_t> counts_;
};

/// An aggregation over the distinct values of its argument. `Get` turns the
/// value counts into the result.
template <class Get>
class ValueCountsFunction final {
public:
  static auto eval(ValueArgs const& args, nova::EvalFrame frame)
    -> nova::Array<nova::Data> {
    return nova::aggregate_lists(args.x, frame,
                                 [](nova::ListElements const& elements,
                                    nova::ArrayBuilder<nova::Data>& builder) {
                                   auto counts = ValueCounts{};
                                   counts.add(elements);
                                   nova::append_data(builder, Get{}(counts));
                                 });
  }

  auto update(ValueArgs const& args, nova::EvalFrame frame) -> void {
    counts_.add(args.x.data, frame.mask());
  }

  auto get() const -> nova::Data {
    return Get{}(counts_);
  }

  auto reset() -> void {
    counts_ = {};
  }

private:
  ValueCounts counts_;
};

} // namespace tenzir::plugins::nova_value_counts

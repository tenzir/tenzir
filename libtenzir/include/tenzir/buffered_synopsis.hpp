//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/bloom_filter_parameters.hpp"
#include "tenzir/error.hpp"
#include "tenzir/synopsis.hpp"

#include <caf/fwd.hpp>

namespace tenzir {

// TODO: Turn this into a concept when we support C++20.
template <typename T>
struct buffered_synopsis_traits {
  // Create a new bloom filter synopsis from the given parameters
  template <typename HashFunction>
  static synopsis_ptr make(tenzir::type type, bloom_filter_parameters p,
                           std::vector<size_t> seeds = {})
    = delete;

  // Estimate the size in bytes for a vector of typed series.
  template <typename SeriesType>
  static size_t memusage(const std::vector<SeriesType>&) = delete;
};

/// A synopsis that stores a full copy of the input in a hash table to be able
/// to construct a smaller bloom filter synopsis for this data at a later
/// point in time using the `shrink` function.
/// @note This is currently used for the active partition: The input is buffered
/// and converted to a bloom filter when the partition is converted to a passive
/// partition and no more entries are expected to be added.
template <class T, class HashFunction>
class buffered_synopsis final : public synopsis {
public:
  using element_type = T;
  using view_type = view<T>;
  using tenzir_type = data_to_type_t<T>;
  using series_type = basic_series<tenzir_type>;

  buffered_synopsis(tenzir::type x, double p) : synopsis{std::move(x)}, p_{p} {
    // nop
  }

  [[nodiscard]] synopsis_ptr clone() const override {
    auto copy = std::make_unique<buffered_synopsis>(type(), p_);
    copy->data_ = data_;
    return copy;
  }

  [[nodiscard]] synopsis_ptr shrink() const override {
    // Count unique values across all series using views (no materialization)
    std::unordered_set<view_type> unique_values;
    for (const auto& s : data_) {
      for (int64_t i = 0; i < s.array->length(); ++i) {
        if (s.array->IsNull(i)) {
          continue;
        }
        auto y = value_at(tenzir_type{}, *s.array, i);
        unique_values.insert(y);
      }
    }
    size_t next_power_of_two = 1ull;
    while (unique_values.size() > next_power_of_two) {
      next_power_of_two *= 2;
    }
    bloom_filter_parameters params;
    params.p = p_;
    params.n = next_power_of_two;
    TENZIR_DEBUG("shrinks buffered synopsis to {} elements", params.n);
    auto type = annotate_parameters(this->type(), params);
    // TODO: If we can get rid completely of the `ip_synopsis` and
    // `string_synopsis` types, we could also call the correct constructor here.
    auto shrunk_synopsis
      = buffered_synopsis_traits<T>::template make<HashFunction>(
        type, std::move(params));
    if (! shrunk_synopsis) {
      return nullptr;
    }
    // Add all buffered series to the bloom filter synopsis
    for (const auto& s : data_) {
      shrunk_synopsis->add(series{s});
    }
    return shrunk_synopsis;
  }

  // Implementation of the remainder of the `synopsis` API.
  void add(const series& x) override {
    auto typed = x.as<tenzir_type>();
    TENZIR_ASSERT(typed);
    TENZIR_ASSERT(typed->array);
    data_.push_back(*typed);
  }

  [[nodiscard]] size_t memusage() const override {
    size_t result = sizeof(p_) + sizeof(data_);
    // Add memory for each series and its underlying Arrow array
    for (const auto& s : data_) {
      result += sizeof(series_type);
      // Sum the sizes of all buffers in the Arrow array
      for (const auto& buffer : s.array->data()->buffers) {
        if (buffer) {
          result += buffer->size();
        }
      }
    }
    return result;
  }

  [[nodiscard]] std::optional<bool>
  lookup(relational_operator op, data_view rhs) const override {
    // This code path should never be reached in normal operation because:
    // 1. Buffered synopses are only used in active partitions
    // 2. Active partitions don't perform synopsis lookups during queries
    // 3. Before catalog lookups, buffered synopses are shrunk to bloom filters
    TENZIR_ERROR("buffered_synopsis::lookup should never be called in normal "
                 "operation");
    TENZIR_UNUSED(op, rhs);
    // Return true (false positive) to avoid breaking queries if somehow called
    return true;
  }

  bool inspect_impl(supported_inspectors&) override {
    TENZIR_ERROR("attempted to inspect a buffered_string_synopsis");
    return false;
  }

  [[nodiscard]] bool equals(const synopsis& other) const noexcept override {
    // This code path should never be reached in normal operation because
    // buffered synopses are never compared.
    TENZIR_ERROR("buffered_synopsis::equals should never be called in normal "
                 "operation");
    TENZIR_UNUSED(other);
    return false;
  }

private:
  double p_;
  std::vector<series_type> data_;
};

} // namespace tenzir

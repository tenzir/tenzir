//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2020 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/bloom_filter_parameters.hpp"
#include "tenzir/bloom_filter_synopsis.hpp"
#include "tenzir/error.hpp"
#include "tenzir/option.hpp"
#include "tenzir/synopsis.hpp"

#include <caf/fwd.hpp>
#include <tsl/robin_set.h>

namespace tenzir {

template <typename T>
struct buffered_synopsis_traits;

/// A synopsis that stores the unique input values in a hash table to be able to
/// construct a smaller bloom filter synopsis for this data at a later point in
/// time using the `shrink` function.
/// @note This is currently used for the active partition: The input is buffered
/// and converted to a bloom filter when the partition is converted to a passive
/// partition and no more entries are expected to be added.
template <class T, class HashFunction>
class buffered_synopsis final : public synopsis {
public:
  using element_type = T;
  using view_type = view<T>;
  using tenzir_type = data_to_type_t<T>;
  using set_type = typename buffered_synopsis_traits<T>::set_type;

  buffered_synopsis(tenzir::type x, double p) : synopsis{std::move(x)}, p_{p} {
    // nop
  }

  [[nodiscard]] synopsis_ptr clone() const override {
    auto copy = std::make_unique<buffered_synopsis>(type(), p_);
    copy->unique_values_ = unique_values_;
    return copy;
  }

  [[nodiscard]] synopsis_ptr shrink() const override {
    size_t next_power_of_two = 1ull;
    while (unique_values_.size() > next_power_of_two) {
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
    if (not shrunk_synopsis) {
      return nullptr;
    }
    auto* bloom = dynamic_cast<bloom_filter_synopsis<T, HashFunction>*>(
      shrunk_synopsis.get());
    TENZIR_ASSERT(bloom);
    for (const auto& x : unique_values_) {
      bloom->add(x);
    }
    return shrunk_synopsis;
  }

  // Implementation of the remainder of the `synopsis` API.
  void add(const series& x) override {
    auto typed = x.as<tenzir_type>();
    TENZIR_ASSERT(typed);
    TENZIR_ASSERT(typed->array);
    for (auto value : typed->values3()) {
      if (value) {
        buffered_synopsis_traits<T>::insert(unique_values_, *value);
      }
    }
  }

  [[nodiscard]] size_t memusage() const override {
    return sizeof(p_) + buffered_synopsis_traits<T>::memusage(unique_values_);
  }

  [[nodiscard]] Option<bool>
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
  set_type unique_values_;
};

} // namespace tenzir

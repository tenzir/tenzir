//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/detail/assert.hpp"
#include "vast/detail/worm.hpp"
#include "vast/fbs/bloom_filter.hpp"
#include "vast/sketch/bloom_filter_config.hpp"

#include <caf/error.hpp>
#include <caf/expected.hpp>
#include <flatbuffers/flatbuffers.h>

#include <cstddef>
#include <cstdint>
#include <span>
#include <type_traits>

namespace vast::sketch {

/// A Bloom filter view, used by mutable and frozen Bloom filter.
///
/// This implementation takes as input an existing hash digests and remixes it
/// k times using worm hashing. Promoted by Peter Dillinger, worm hashing
/// stands in contrast to standard Bloom filter implementations that hash a
/// value k times or use double hashing. Worm hashing is superior becase it
/// never wastes hash entropy.
template <class Word>
struct bloom_filter_view {
  static_assert(
    std::is_same_v<Word, uint64_t> || std::is_same_v<Word, const uint64_t>);

  /// Default-constructs an invalid view.
  bloom_filter_view() {
    params_.m = 0;
    params_.n = 0;
    params_.k = 0;
    params_.p = 1;
  };

  /// Constructs a view from Bloom filter parameters and a span of bytes.
  bloom_filter_view(bloom_filter_params params, std::span<Word> bits)
    : params_{params}, bits_{bits} {
    VAST_ASSERT(params.m & 1, "worm hashing requires odd m");
  }

  /// Adds a hash digest to the filter.
  void add(uint64_t digest) noexcept {
    for (size_t i = 0; i < params_.k; ++i) {
      auto [upper, lower] = vast::detail::wide_mul(params_.m, digest);
      bits_[upper >> 6] |= (uint64_t{1} << (upper & 63));
      digest = lower;
    }
  }

  /// Checks whether a hash digest exists in the filter.
  bool lookup(uint64_t digest) const noexcept {
    for (size_t i = 0; i < params_.k; ++i) {
      auto [upper, lower] = vast::detail::wide_mul(params_.m, digest);
      if ((bits_[upper >> 6] & (uint64_t{1} << (upper & 63))) == 0)
        return false;
      digest = lower;
    }
    return true;
  }

  /// Retrieves the Bloom filter paramers.
  const bloom_filter_params& parameters() const noexcept {
    return params_;
  }

  friend size_t mem_usage(const bloom_filter_view& x) noexcept {
    return sizeof(x.params_) + sizeof(x.bits_);
  }

  friend caf::expected<flatbuffers::Offset<fbs::BloomFilter>>
  pack(flatbuffers::FlatBufferBuilder& builder,
       const bloom_filter_view& x) noexcept {
    auto params = fbs::BloomFilterParameters{x.params_.m, x.params_.n,
                                             x.params_.k, x.params_.p};
    auto bits_offset = builder.CreateVector(x.bits_.data(), x.bits_.size());
    return fbs::CreateBloomFilter(builder, &params, bits_offset);
  }

  friend caf::error
  unpack(const fbs::BloomFilter& table, bloom_filter_view& x) noexcept {
    x.params_.m = table.parameters()->m();
    x.params_.n = table.parameters()->n();
    x.params_.k = table.parameters()->k();
    x.params_.p = table.parameters()->p();
    x.bits_ = std::span{table.bits()->data(), table.bits()->size()};
    return {};
  }

  bloom_filter_params params_;
  std::span<Word> bits_;
};

using mutable_bloom_filter_view = bloom_filter_view<uint64_t>;
using immutable_bloom_filter_view = bloom_filter_view<const uint64_t>;

} // namespace vast::sketch

//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/bloom_filter_parameters.hpp"
#include "vast/chunk.hpp"

#include <caf/expected.hpp>
#include <caf/meta/type_name.hpp>

#include <cstddef>
#include <vector>

namespace vast::sketch {

class frozen_bloom_filter;

/// A Bloom filter that operates on 64-bit hash digests and uses *worm hashing*
/// to perform k-fold rehashing.
class bloom_filter {
public:
  /// Constructs a Bloom filter from a set of evaluated parameters.
  /// @param *xs* The desired Bloom filter parameters.
  /// @returns The Bloom filter for *xs* iff the parameterization is valid.
  static caf::expected<bloom_filter> make(bloom_filter_parameters xs);

  /// Adds a hash digest to the Bloom filter.
  /// @param digest The digest to add.
  void add(uint64_t digest) noexcept;

  /// Test whether a hash digest is in the Bloom filter.
  /// @param digest The digest to test.
  /// @returns `false` if the *digest* is not in the set and `true` if *digest*
  /// may exist according to the false-positive probability of the filter.
  bool lookup(uint64_t digest) const noexcept;

  /// Retrieves the parameters of the filter.
  const bloom_filter_parameters& parameters() const noexcept;

  // -- concepts --------------------------------------------------------------

  /// @returns An estimate for amount of memory (in bytes) used by this filter.
  friend size_t mem_usage(const bloom_filter& x);

  template <class Inspector>
  friend auto inspect(Inspector& f, bloom_filter& x) {
    return f(caf::meta::type_name("bloom_filter"), x.params_, x.bits_);
  }

  friend caf::expected<frozen_bloom_filter> freeze(const bloom_filter& x);

private:
  explicit bloom_filter(bloom_filter_parameters params);

  bloom_filter_parameters params_;
  std::vector<uint64_t> bits_;
};

/// An immutable Bloom filter wrapped in a contiguous chunk of memory.
class frozen_bloom_filter {
public:
  explicit frozen_bloom_filter(chunk_ptr table) noexcept;

  bool lookup(uint64_t digest) const noexcept;

  /// Retrieves the parameters of the filter.
  bloom_filter_parameters parameters() const noexcept;

  friend size_t mem_usage(const frozen_bloom_filter& x) noexcept;

private:
  chunk_ptr table_;
};

} // namespace vast::sketch

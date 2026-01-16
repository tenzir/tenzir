//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause
//
// This Bloom filter takes as input an existing hash digests and remixes it k
// times using worm hashing. Promoted by Peter Dillinger, worm hashing stands
// in contrast to standard Bloom filter implementations that hash a value k
// times or use double hashing. Worm hashing is superior becase it never wastes
// hash entropy.
//
#pragma once

#include "tenzir/chunk.hpp"
#include "tenzir/sketch/bloom_filter_config.hpp"
#include "tenzir/sketch/bloom_filter_view.hpp"

#include <caf/expected.hpp>

#include <cstddef>
#include <cstdint>
#include <vector>

namespace tenzir::sketch {

/// An immutable Bloom filter wrapped in a contiguous chunk of memory.
class frozen_bloom_filter {
public:
  /// Constructs a frozen Bloom filter from a flatbuffer.
  /// @pre *table* must be a valid Bloom filter flatbuffer.
  explicit frozen_bloom_filter(chunk_ptr table) noexcept;

  /// Test whether a hash digest is in the Bloom filter.
  /// @param digest The digest to test.
  /// @returns `false` if the *digest* is not in the set and `true` if *digest*
  /// may exist according to the false-positive probability of the filter.
  bool lookup(uint64_t digest) const noexcept;

  /// Retrieves the parameters of the filter.
  const bloom_filter_params& parameters() const noexcept;

  // -- concepts --------------------------------------------------------------

  friend size_t mem_usage(const frozen_bloom_filter& x) noexcept;

private:
  immutable_bloom_filter_view view_;
  chunk_ptr table_;
};

/// A mutable Bloom filter.
class bloom_filter {
public:
  /// Constructs a Bloom filter from a set of evaluated parameters.
  /// @param *cfg* The desired Bloom filter configuration.
  /// @returns The Bloom filter for *cfg* iff the parameterization is valid.
  static caf::expected<bloom_filter> make(bloom_filter_config cfg);

  /// Adds a hash digest to the Bloom filter.
  /// @param digest The digest to add.
  void add(uint64_t digest) noexcept;

  /// Test whether a hash digest is in the Bloom filter.
  /// @param digest The digest to test.
  /// @returns `false` if the *digest* is not in the set and `true` if *digest*
  /// may exist according to the false-positive probability of the filter.
  bool lookup(uint64_t digest) const noexcept;

  /// Retrieves the parameters of the filter.
  const bloom_filter_params& parameters() const noexcept;

  // -- concepts --------------------------------------------------------------

  friend size_t mem_usage(const bloom_filter& x);

  friend caf::expected<frozen_bloom_filter> freeze(const bloom_filter& x);

private:
  explicit bloom_filter(bloom_filter_params params);

  bloom_filter_params params_;
  std::vector<uint64_t> bits_;
};

} // namespace tenzir::sketch

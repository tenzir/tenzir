//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/bloom_filter.hpp"
#include "tenzir/dcso_bloom_hasher.hpp"
#include "tenzir/detail/operators.hpp"
#include "tenzir/hash/fnv.hpp"

#include <caf/error.hpp>

#include <cstddef>
#include <span>
#include <vector>

namespace tenzir {

/// A Bloom filter that is binary-compatible to https://github.com/DCSO/bloom.
class dcso_bloom_filter : detail::equality_comparable<dcso_bloom_filter> {
public:
  using bloom_filter_type
    = bloom_filter<fnv1<64>, dcso_bloom_hasher, policy::partitioning::no>;
  using hasher_type = bloom_filter_type::hasher_type;

  /// Computes the number of cells in the underlying Bloom filter.
  /// @pre `p > 0 && p < 1`
  /// @pre `n > 0`
  static auto m(uint64_t n, double p) -> uint64_t;

  /// Computes the (optimal) number of hash functions in the underlying Bloom
  /// filter.
  /// @pre `n > 0`
  static auto k(uint64_t n, double p) -> uint64_t;

  /// Default-constructs a tiny Bloom filter with *n = 1* and *p = 0.5*.
  /// This constructor shall not perform memory allocations and exists only to
  /// simplify assignment/deserialization.
  dcso_bloom_filter();

  /// Constructs a Bloom filter for a fixed number of elements and given
  /// false-positive probability.
  /// @param n The capacity of the Bloom filter.
  /// @paramp The false-positive probability
  /// @pre `p > 0 && p < 1`
  dcso_bloom_filter(uint64_t n, double p);

  /// Adds a value to the filter.
  /// @returns `true` iff a new element was added.
  template <class T>
  auto add(const T& x) -> bool {
    if (!bloom_filter_.add(x)) {
      return false;
    }
    ++num_elements_;
    return true;
  }

  /// Looks up value in the filter.
  template <class T>
  auto lookup(const T& x) const -> bool {
    return bloom_filter_.lookup(x);
  }

  /// Returns the Bloom filter parameters.
  auto parameters() const -> const bloom_filter_parameters&;

  /// Retrieves an estimate the number of elements in the Bloom filter.
  auto num_elements() const -> uint64_t;

  /// Access the attached data.
  auto data() const -> const std::vector<std::byte>&;

  /// Access the attached data.
  auto data() -> std::vector<std::byte>&;

  // -- concepts --------------------------------------------------------------

  template <class Inspector>
  friend auto inspect(Inspector& f, dcso_bloom_filter& x) {
    return f.object(x)
      .pretty_name("dcso_bloom_filter")
      .fields(f.field("bloom_filter", x.bloom_filter_),
              f.field("params", x.params_),
              f.field("num_elements", x.num_elements_),
              f.field("data", x.data_));
  }

  friend auto operator==(const dcso_bloom_filter& x, const dcso_bloom_filter& y)
    -> bool;

  friend auto convert(std::span<const std::byte> xs, dcso_bloom_filter& x)
    -> caf::error;

  friend auto convert(const dcso_bloom_filter& x, std::vector<std::byte>& xs)
    -> caf::error;

private:
  /// Version (1) + Bloom filter parameters (4) + #elements (1).
  static constexpr size_t header_bytes = 6 * 8;

  /// The minimum number of bytes that the bits of a Bloom filter can occupy.
  /// We determined this value empirically by parameterizing m(n, p) with
  /// small values. See the unit tests.
  static constexpr size_t min_filter_bytes = 1;

  /// The minimum number of bytes we need in order to have a well-defined DCSO
  /// Bloom filter.
  static constexpr size_t min_buffer_size = header_bytes + min_filter_bytes;

  class serializer;
  class deserializer;

  /// The underlying Bloom filter with FNV-1 hash.
  bloom_filter_type bloom_filter_;

  /// The Bloom filter parameters.
  bloom_filter_parameters params_{};

  /// Unique number of inserted elements.
  /// @note Called `N` in DCSO's bloom.
  uint64_t num_elements_{0};

  /// Arbitrary data that can be attached.
  /// @note Specific to DCSO's bloom.
  std::vector<std::byte> data_{};
};

} // namespace tenzir

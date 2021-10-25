//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/bitvector.hpp"
#include "vast/bloom_filter_parameters.hpp"
#include "vast/detail/operators.hpp"
#include "vast/hash/hasher.hpp"
#include "vast/legacy_type.hpp"
#include "vast/logger.hpp"

#include <caf/meta/load_callback.hpp>
#include <caf/meta/type_name.hpp>

#include <climits>
#include <cstddef>
#include <numeric>
#include <optional>
#include <type_traits>
#include <utility>
#include <vector>

namespace vast::policy {

/// A policy that controls the cell layout of a Bloom filter.
/// If `yes`, the Bloom filter bits are split into *k* equi-distant partitions.
/// @relates bloom_filter
enum class partitioning { yes, no };

} // namespace vast::policy

namespace vast {

/// A data structure for probabilistic set membership.
/// @tparam HashFunction The hash function to use in the hasher.
/// @tparam Hasher The hasher type to generate digests.
/// @tparam Partitioning The partitioning policy.
template <class HashFunction, template <class> class Hasher = double_hasher,
          policy::partitioning Partitioning = policy::partitioning::no>
class bloom_filter : detail::equality_comparable<
                       bloom_filter<HashFunction, Hasher, Partitioning>> {
public:
  using hash_function = HashFunction;
  using hasher_type = Hasher<hash_function>;

  static constexpr policy::partitioning partitioning_policy = Partitioning;

  /// Constructs a Bloom filter with a fixed size and a hasher.
  /// @param size The number of cells/bits in the Bloom filter.
  /// @param hasher The hasher type to generate digests.
  explicit bloom_filter(size_t size = 0, hasher_type hasher = hasher_type{})
    : hasher_{std::move(hasher)}, bits_(size) {
    // nop
  }

  /// Adds an element to the Bloom filter.
  /// @param x The element to add.
  /// @returns `false` iff *x* already exists in the filter.
  template <class T>
  bool add(T&& x) {
    auto& digests = hasher_(std::forward<T>(x));
    auto unique = false;
    for (size_t i = 0; i < digests.size(); ++i) {
      auto bit = bits_[position(i, digests[i])];
      unique |= bit == false;
      bit = true;
    }
    return unique;
  }

  /// Test whether an element exists in the Bloom filter.
  /// @param x The element to test.
  /// @returns `false` if the *x* is not in the set and `true` if *x* may exist
  ///          according to the false-positive probability of the filter.
  template <class T>
  bool lookup(T&& x) const {
    auto& digests = hasher_(std::forward<T>(x));
    for (size_t i = 0; i < digests.size(); ++i)
      if (!bits_[position(i, digests[i])])
        return false;
    return true;
  }

  /// @returns The number of cells in the underlying bit vector.
  [[nodiscard]] size_t size() const {
    return bits_.size();
  }

  /// @returns An estimate for amount of memory (in bytes) used by this filter.
  [[nodiscard]] size_t memusage() const {
    return sizeof(bloom_filter) + bits_.capacity() / CHAR_BIT;
  }

  /// @returns The number of hash functions in the hasher.
  [[nodiscard]] size_t num_hash_functions() const {
    return hasher_.size();
  }

  // -- concepts --------------------------------------------------------------

  friend bool operator==(const bloom_filter& x, const bloom_filter& y) {
    return x.hasher_ == y.hasher_ && x.bits_ == y.bits_;
  }

  template <class Inspector>
  friend auto inspect(Inspector& f, bloom_filter& x) {
    auto load_callback = caf::meta::load_callback([&]() -> caf::error {
      // When deserializing into a vector that was already bigger than
      // required, CAF will reuse the storage but not release the
      // excess afterwards.
      x.bits_.shrink_to_fit();
      return caf::none;
    });
    return f(caf::meta::type_name("bloom_filter"), x.hasher_, x.bits_,
             std::move(load_callback));
  }

private:
  template <class Digest>
  size_t position([[maybe_unused]] size_t i, Digest x) const {
    if constexpr (partitioning_policy == policy::partitioning::no)
      return x % bits_.size();
    if constexpr (partitioning_policy == policy::partitioning::yes) {
      auto num_partition_cells = bits_.size() / hasher_.size();
      return i * num_partition_cells + (x % num_partition_cells);
    }
  }

  hasher_type hasher_;
  bitvector<uint64_t> bits_;
};

/// Constructs a Bloom filter for a given set of parameters.
/// @tparam HashFunction The hash function to use in the hasher.
/// @tparam Hasher The hasher type to generate digests.
/// @tparam Partitioning The partitioning policy.
/// @param xs The Bloom filter parameters.
/// @param seeds The seeds for the hash functions. If empty, ascending
///              integers from 0 to *k-1* will be used.
/// @relates bloom_filter bloom_filter_parameters
template <class HashFunction, template <class> class Hasher = double_hasher,
          policy::partitioning Partitioning = policy::partitioning::no>
std::optional<bloom_filter<HashFunction, Hasher, Partitioning>>
make_bloom_filter(bloom_filter_parameters xs, std::vector<size_t> seeds = {}) {
  using result_type = bloom_filter<HashFunction, Hasher, Partitioning>;
  using hasher_type = typename result_type::hasher_type;
  if (auto ys = evaluate(xs)) {
    VAST_DEBUG("evaluated bloom filter parameters: {} {} {} {}",
               VAST_ARG(ys->k), VAST_ARG(ys->m), VAST_ARG(ys->n),
               VAST_ARG(ys->p));
    if (*ys->m == 0 || *ys->k == 0)
      return {};
    if (seeds.empty()) {
      if constexpr (std::is_same_v<hasher_type, double_hasher<HashFunction>>) {
        seeds = {0, 1};
      } else {
        seeds.resize(*ys->k);
        std::iota(seeds.begin(), seeds.end(), 0);
      }
    } else if (seeds.size() != *ys->k) {
      return {};
    }
    return result_type{*ys->m, hasher_type{*ys->k, std::move(seeds)}};
  }
  return {};
}

} // namespace vast

/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#pragma once

#include "vast/bitvector.hpp"
#include "vast/bloom_filter_parameters.hpp"
#include "vast/detail/operators.hpp"
#include "vast/hasher.hpp"
#include "vast/logger.hpp"

#include <caf/meta/load_callback.hpp>
#include <caf/meta/type_name.hpp>
#include <caf/optional.hpp>

#include <climits>
#include <cstddef>
#include <numeric>
#include <type_traits>
#include <utility>
#include <vector>

namespace vast::policy {

/// A tag type for choosing partitioning of a Bloom filter. This
/// policy slices the Bloom filter bits into *k* equi-distant partitions.
/// @relates bloom_filter
struct partitioning;

/// A tag type for avoiding partitioning in a Bloom filter. This policy
/// considers the Bloom filter bits as one contiguous chunk.
/// @relates bloom_filter
struct no_partitioning;

} // namespace vast::policy

namespace vast {

/// A data structure for probabilistic set membership.
/// @tparam HashFunction The hash function to use in the hasher.
/// @tparam Hasher The hasher type to generate digests.
/// @tparam PartitioningPolicy The partitioning policy.
template <class HashFunction, template <class> class Hasher = double_hasher,
          class PartitioningPolicy = policy::no_partitioning>
class bloom_filter : detail::equality_comparable<
                       bloom_filter<HashFunction, Hasher, PartitioningPolicy>> {
public:
  using hash_function = HashFunction;
  using hasher_type = Hasher<hash_function>;
  using partitioning_policy = PartitioningPolicy;

  /// Constructs a Bloom filter with a fixed size and a hasher.
  /// @param size The number of cells/bits in the Bloom filter.
  /// @param hasher The hasher type to generate digests.
  explicit bloom_filter(size_t size = 0, hasher_type hasher = hasher_type{})
    : hasher_{std::move(hasher)}, bits_(size) {
    // nop
  }

  /// Adds an element to the Bloom filter.
  /// @param x The element to add.
  template <class T>
  void add(T&& x) {
    auto& digests = hasher_(std::forward<T>(x));
    for (size_t i = 0; i < digests.size(); ++i)
      bits_[position(i, digests[i])] = true;
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
  size_t size() const {
    return bits_.size();
  }

  /// @returns An estimate for amount of memory (in bytes) used by this filter.
  size_t size_bytes() const {
    return sizeof(bloom_filter) + bits_.capacity() / CHAR_BIT;
  }

  /// @returns The number of hash functions in the hasher.
  size_t num_hash_functions() const {
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
    if constexpr (std::is_same_v<partitioning_policy, policy::no_partitioning>)
      return x % bits_.size();
    if constexpr (std::is_same_v<partitioning_policy, policy::partitioning>) {
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
/// @tparam PartitioningPolicy The partitioning policy.
/// @param xs The Bloom filter parameters.
/// @param seeds The seeds for the hash functions. If empty, ascending
///              integers from 0 to *k-1* will be used.
/// @relates bloom_filter bloom_filter_parameters
template <class HashFunction, template <class> class Hasher = double_hasher,
          class PartitioningPolicy = policy::no_partitioning>
caf::optional<bloom_filter<HashFunction, Hasher, PartitioningPolicy>>
make_bloom_filter(bloom_filter_parameters xs, std::vector<size_t> seeds = {}) {
  using result_type = bloom_filter<HashFunction, Hasher, PartitioningPolicy>;
  using hasher_type = typename result_type::hasher_type;
  if (auto ys = evaluate(xs)) {
    VAST_DEBUG_ANON("evaluated bloom filter parameters:", VAST_ARG(ys->k),
                    VAST_ARG(ys->m), VAST_ARG(ys->n), VAST_ARG(ys->p));
    if (*ys->m == 0 || *ys->k == 0)
      return caf::none;
    if (seeds.empty()) {
      if constexpr (std::is_same_v<hasher_type, double_hasher<HashFunction>>) {
        seeds = {0, 1};
      } else {
        seeds.resize(*ys->k);
        std::iota(seeds.begin(), seeds.end(), 0);
      }
    } else if (seeds.size() != *ys->k) {
      return caf::none;
    }
    return result_type{*ys->m, hasher_type{*ys->k, std::move(seeds)}};
  }
  return caf::none;
}

} // namespace vast

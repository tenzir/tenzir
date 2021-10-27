//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/detail/assert.hpp"
#include "vast/detail/operators.hpp"
#include "vast/hash/hash.hpp"

#include <caf/sec.hpp>

#include <cstddef>
#include <utility>
#include <vector>

namespace vast {

/// The CRTP base class for hashers.
template <class Derived, class DigestType>
class hasher {
public:
  using result_type = const std::vector<DigestType>&;

  /// Constructs a hasher that computes a fixed number of hash digests.
  /// @param k The number of hash digests to compute.
  /// @pre `k > 0`
  explicit hasher(size_t k) : digests_(k) {
    VAST_ASSERT(k > 0);
  }

  /// Computes a digest for a hasheable object.
  /// @tparam A type that models the `Hashable` concept.
  /// @returns *k* hash digests.
  /// @post `k == size()`
  template <class T>
  result_type operator()(const T& x) const {
    auto derived = static_cast<const Derived*>(this);
    const_cast<Derived*>(derived)->hash(x, digests_);
    return digests_;
  }

  /// @returns The number of hash digests this hasher computes.
  size_t size() const {
    return digests_.size();
  }

  // -- concepts -------------------------------------------------------------

  template <class Inspector>
  friend typename Inspector::result_type inspect(Inspector& f, hasher& x) {
    uint32_t k;
    if constexpr (Inspector::reads_state) {
      k = static_cast<uint32_t>(x.size());
      return f(k);
    } else {
      static_assert(Inspector::writes_state);
      auto cb = [&]() -> caf::error {
        if (k == 0)
          return caf::sec::invalid_argument;
        x.digests_.resize(k);
        return caf::none;
      };
      return f(k, caf::meta::load_callback(cb));
    }
  }

private:
  mutable std::vector<DigestType> digests_;
};

/// A hasher that computes *k* digests with *k* hash functions.
template <class HashFunction>
class simple_hasher
  : public hasher<simple_hasher<HashFunction>,
                  typename HashFunction::result_type>,
    vast::detail::equality_comparable<simple_hasher<HashFunction>> {
  using super
    = hasher<simple_hasher<HashFunction>, typename HashFunction::result_type>;

public:
  /// Constructs a hasher from a vector of hash functions.
  /// @param k The number of hash functions to use.
  /// @param xs The vector with seeds for the hash functions.
  /// @pre `!xs.empty()`
  explicit simple_hasher(size_t k = 1, std::vector<size_t> xs = {0})
    : super{k}, seeds_{std::move(xs)} {
    VAST_ASSERT(k == seeds_.size());
  }

  /// Hashes a value *k* times with *k* hash functions.
  /// @param T x The value to hash.
  /// @param Ts xs The sequence to write the digests into.
  template <class T, class Ts>
  void hash(const T& x, Ts& xs) {
    VAST_ASSERT(xs.size() == seeds_.size());
    for (size_t i = 0; i < xs.size(); ++i)
      xs[i] = seeded_hash<HashFunction>{seeds_[i]}(x);
  }

  // -- concepts -------------------------------------------------------------

  friend bool operator==(const simple_hasher& x, const simple_hasher& y) {
    return x.seeds_ == y.seeds_;
  }

  template <class Inspector>
  friend auto inspect(Inspector& f, simple_hasher& x) {
    return f(caf::meta::type_name("simple_hasher"), static_cast<super&>(x),
             x.seeds_);
  }

private:
  std::vector<size_t> seeds_;
};

/// A hasher that uses *double hashing* to compute multiple digests.
template <class HashFunction>
class double_hasher
  : public hasher<double_hasher<HashFunction>,
                  typename HashFunction::result_type>,
    vast::detail::equality_comparable<double_hasher<HashFunction>> {
  using super
    = hasher<double_hasher<HashFunction>, typename HashFunction::result_type>;

public:
  /// Constructs a hasher with a vector of seeds.
  /// @param k The number of hash functions to use.
  /// @param xs The vector with seeds for the hash functions.
  explicit double_hasher(size_t k = 2, const std::vector<size_t>& xs = {0, 1})
    : super{k} {
    VAST_ASSERT(xs.size() == 2);
    seed1_ = xs[0];
    seed2_ = xs[1];
  }

  /// Hashes a value *k* times with *2* hash functions via *double hashing*.
  /// @param T x The value to hash.
  /// @param Ts xs The sequence to write the digests into.
  template <class T, class Ts>
  void hash(const T& x, Ts& xs) {
    auto d1 = seeded_hash<HashFunction>{seed1_}(x);
    auto d2 = seeded_hash<HashFunction>{seed2_}(x);
    for (size_t i = 0; i < xs.size(); ++i)
      xs[i] = d1 + i * d2;
  }

  // -- concepts -------------------------------------------------------------

  friend bool operator==(const double_hasher& x, const double_hasher& y) {
    return x.size() == y.size() && x.seed1_ == y.seed1_ && x.seed2_ == y.seed2_;
  }

  template <class Inspector>
  friend auto inspect(Inspector& f, double_hasher& x) {
    return f(caf::meta::type_name("double_hasher"), static_cast<super&>(x),
             x.seed1_, x.seed2_);
  }

private:
  size_t seed1_;
  size_t seed2_;
};

} // namespace vast

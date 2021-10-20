//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/concept/hashable/hash.hpp"
#include "vast/concept/hashable/xxhash.hpp"
#include "vast/concepts.hpp"
#include "vast/data.hpp"
#include "vast/detail/assert.hpp"
#include "vast/detail/overload.hpp"
#include "vast/detail/stable_map.hpp"
#include "vast/detail/type_traits.hpp"
#include "vast/logger.hpp"
#include "vast/value_index.hpp"
#include "vast/view.hpp"

#include <caf/deserializer.hpp>
#include <caf/expected.hpp>
#include <caf/serializer.hpp>
#include <caf/settings.hpp>
#include <tsl/robin_map.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstring>
#include <optional>
#include <string>
#include <type_traits>
#include <unordered_set>
#include <vector>

namespace vast {

/// An index that only supports equality lookup by hashing its data. The
/// hash_index computes a digest of the input data and concatenates all digests
/// in a sequence. Optionally, it chops off the values after a fixed number of
/// bytes for a more space-efficient representation, at the cost of more false
/// positives. A separate "satellite structure" keeps track of hash collision
/// to make the index exact. The additional state to build this satellite
/// structure only exists during the construction of the index. Upon
/// descruction, this extra state ceases to exist and it will not be possible
/// to append further values when deserializing an existing index.
template <size_t Bytes>
class hash_index : public value_index {
  static_assert(Bytes > 0, "cannot use 0 bytes to store a digest");

  // We're chopping off the actual hash digest such that it fits into a 64-bit
  // integeger. Hence, we do not support more than 8 bytes at this point. This
  // is not a fundamental limitation, but we don't need more than 8 bytes
  // either. The reason is that 64 bits allow this index to store sqrt(2^64) =
  // 2^32 unique values before collisions are expected. In other words, 64 bits
  // support ~4B unique values efficiently, which is roughly an order of
  // magnitude less than our typical partition size.
  static_assert(Bytes <= 8, "digests > 8 bytes not supported");

  /// The maximum number of hash rounds to try to find a new digest.
  static constexpr size_t max_hash_rounds = 32;

public:
  // TODO: switch to XXH3 once the persistent index layout is versioned and
  // upgradable. Until then we have to support existing state that uses XXH64.
  using hash_algorithm = xxh64;
  using digest_type = std::array<std::byte, Bytes>;

  static_assert(sizeof(hash_algorithm::result_type) >= Bytes,
                "number of chosen bytes exceeds underlying digest size");

  /// Computes a chopped digest from arbitrary data.
  /// @param x The data to hash.
  /// @param seed The seed to use during the hash.
  /// @returns The chopped digest.
  static digest_type hash(data_view x, size_t seed = 0) {
    digest_type result;
    auto digest = seeded_hash<hash_algorithm>{seed}(x);
    std::memcpy(result.data(), &digest, Bytes);
    return result;
  }

  /// Constructs a hash index for a particular type and digest cutoff.
  /// @param t The type associated with this index.
  /// @param opts Runtime context for index parameterization.
  explicit hash_index(vast::legacy_type t, caf::settings opts = {})
    : value_index{std::move(t), std::move(opts)} {
  }

  caf::error serialize(caf::serializer& sink) const override {
    // Prune unneeded seeds.
    decltype(seeds_) non_null_seeds;
    for (auto& [k, v] : seeds_)
      if (v > 0)
        non_null_seeds.emplace(k, v);
    return caf::error::eval([&] { return value_index::serialize(sink); },
                            [&] { return sink(digests_, non_null_seeds); });
  }

  caf::error deserialize(caf::deserializer& source) override {
    return caf::error::eval([&] { return value_index::deserialize(source); },
                            [&] { return source(digests_, seeds_); });
  }

  const std::vector<digest_type>& digests() const {
    return digests_;
  }

private:
  struct key {
    friend bool operator==(key x, digest_type y) {
      return std::memcmp(x.bytes.data(), y.data(), y.size()) == 0;
    }

    friend bool operator!=(key x, digest_type y) {
      return !(x == y);
    }

    friend bool operator==(digest_type x, key y) {
      return y == x;
    }

    friend bool operator!=(digest_type x, key y) {
      return !(x == y);
    }

    friend bool operator==(key x, key y) {
      return x == y.bytes;
    }

    friend bool operator!=(key x, key y) {
      return !(x == y);
    }

    digest_type bytes;
  };

  struct key_hasher {
    auto operator()(const key& x) const {
      auto result = uint64_t{0};
      std::memcpy(&result, x.bytes.data(), x.bytes.size());
      return result;
    }
  };

  // Retrieves the unique digest for a given input or generates a new one.
  std::optional<key> make_digest(data_view x) {
    for (size_t i = 0; i < max_hash_rounds; ++i) {
      // Compute a hash digest.
      auto digest = hash(x, i);
      auto k = key{digest};
      // If we have never seen this digest before, we're adding it to the list
      // of seen digests and are done.
      if (unique_digests_.count(k) == 0) {
        // TODO: It should be possible to avoid the `materialize()` here if
        // `seeds_` could be changed to use `data_view` as key type.
        auto result = seeds_.emplace(materialize(x), i);
        VAST_ASSERT(result.second);
        unique_digests_.insert(k);
        return k;
      };
      // If we have seen the digest, check whether we also have a known
      // preimage.
      if (auto it = seeds_.find(x); it != seeds_.end())
        return key{hash(x, it->second)};
    }
    return {};
  }

  /// Locates the digest for a given input.
  key find_digest(data_view x) const {
    auto i = seeds_.find(x);
    return key{i != seeds_.end() ? hash(x, i->second) : hash(x, 0)};
  }

  bool append_impl(data_view x, id) override {
    // After we deserialize the index, we can no longer append data.
    if (immutable())
      return false;
    auto digest = make_digest(x);
    if (!digest)
      return false;
    digests_.push_back(digest->bytes);
    return true;
  }

  [[nodiscard]] caf::expected<ids>
  lookup_impl(relational_operator op, data_view x) const override {
    VAST_ASSERT(rank(this->mask()) == digests_.size());
    // Implementation of the one-pass search algorithm that computes the
    // resulting ID set. The predicate depends on the operator and RHS.
    auto scan = [&](auto predicate) -> ids {
      ewah_bitmap result;
      auto rng = select(this->mask());
      if (rng.done())
        return result;
      for (size_t i = 0, last_match = 0; i < digests_.size(); ++i) {
        if (predicate(digests_[i])) {
          auto digests_since_last_match = i - last_match;
          if (digests_since_last_match > 0)
            rng.next(digests_since_last_match);
          result.append_bits(false, rng.get() - result.size());
          result.append_bit(true);
          last_match = i;
        }
      }
      return result;
    };
    if (op == relational_operator::equal
        || op == relational_operator::not_equal) {
      auto k = find_digest(x);
      auto eq = [=](const digest_type& digest) { return k == digest; };
      auto ne = [=](const digest_type& digest) { return k != digest; };
      return op == relational_operator::equal ? scan(eq) : scan(ne);
    }
    if (op == relational_operator::in || op == relational_operator::not_in) {
      // Ensure that the RHS is a list of strings.
      auto keys = caf::visit(
        detail::overload{
          [&](auto xs) -> caf::expected<std::vector<key>> {
            using view_type = decltype(xs);
            if constexpr (std::is_same_v<view_type, view<list>>) {
              std::vector<key> result;
              result.reserve(xs.size());
              for (auto x : xs)
                result.emplace_back(find_digest(x));
              return result;
            } else {
              return caf::make_error(ec::type_clash, "expected list on RHS",
                                     materialize(x));
            }
          },
        },
        x);
      if (!keys)
        return keys.error();
      // We're good to go with: create the set predicates an run the scan.
      auto in_pred = [&](const digest_type& digest) {
        auto cmp = [=](auto& k) { return k == digest; };
        return std::any_of(keys->begin(), keys->end(), cmp);
      };
      auto not_in_pred = [&](const digest_type& digest) {
        auto cmp = [=](auto& k) { return k == digest; };
        return std::none_of(keys->begin(), keys->end(), cmp);
      };
      return op == relational_operator::in ? scan(in_pred) : scan(not_in_pred);
    }
    return caf::make_error(ec::unsupported_operator, op);
  }

  [[nodiscard]] size_t memusage_impl() const override {
    return digests_.capacity() * sizeof(digest_type)
           + unique_digests_.size() * sizeof(key)
           + seeds_.size() * sizeof(typename decltype(seeds_)::value_type);
  }

  [[nodiscard]] bool immutable() const {
    return unique_digests_.empty() && !digests_.empty();
  }

  std::vector<digest_type> digests_;
  std::unordered_set<key, key_hasher> unique_digests_;

  // We use a robin_map here because it supports heterogenous lookup, which
  // has a major performance impact for `seeds_`, see ch13760.
  using seeds_map = tsl::robin_map<data, size_t>;
  static_assert(concepts::transparent<seeds_map::key_equal>);
  seeds_map seeds_;
};

} // namespace vast

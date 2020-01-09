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

#include "vast/concept/hashable/uhash.hpp"
#include "vast/concept/hashable/xxhash.hpp"
#include "vast/data.hpp"
#include "vast/detail/assert.hpp"
#include "vast/detail/steady_map.hpp"
#include "vast/detail/type_traits.hpp"
#include "vast/value_index.hpp"
#include "vast/view.hpp"

#include <caf/deserializer.hpp>
#include <caf/expected.hpp>
#include <caf/optional.hpp>
#include <caf/serializer.hpp>

#include <algorithm>
#include <array>
#include <cstring>
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
  static_assert(Bytes > 0);
  static_assert(Bytes <= 8);

  /// The maximum number of hash rounds to try to find a new digest.
  static constexpr size_t max_hash_rounds = 32;

public:
  using hasher_type = xxhash64;
  using digest_type = std::array<byte, Bytes>;

  static_assert(sizeof(hasher_type::result_type) >= sizeof(digest_type),
                "number of chosen bytes exceeds underlying digest size");

  /// Computes a chopped digest from arbitrary data.
  /// @param x The data to hash.
  /// @param seed The seed to use during the hash.
  /// @returns The chopped digest.
  static digest_type hash(data_view x, size_t seed = 0) {
    digest_type result;
    auto digest = uhash<hasher_type>{seed}(x);
    std::memcpy(result.data(), &digest, Bytes);
    return result;
  }

  /// Constructs a hash index for a particular type and digest cutoff.
  /// @param t The type associated with this index.
  /// @param digest_bytes The number of bytes to keep of a hash digest.
  explicit hash_index(vast::type t) : value_index{std::move(t)} {
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

  // TODO: remove this function as of C++20.
  auto find_seed(data_view x) const {
    auto pred = [&](const auto& xs) { return is_equal(xs.first, x); };
    auto& xs = as_vector(seeds_);
    return std::find_if(xs.begin(), xs.end(), pred);
  }

  // Retrieves the unique digest for a given input or generates a new one.
  caf::optional<key> make_digest(data_view x) {
    for (size_t i = 0; i < max_hash_rounds; ++i) {
      // Compute a hash digest.
      auto digest = hash(x, i);
      auto k = key{digest};
      // If we have never seen this digest before, we're adding it to the list
      // of seen digests and are done.
      if (unique_digests_.count(k) == 0) {
        auto materialized = materialize(x);
        VAST_ASSERT(seeds_.count(materialized) == 0);
        seeds_.emplace(std::move(materialized), i);
        unique_digests_.insert(k);
        return k;
      };
      // If we have seen the digest, check whether we also have a known
      // preimage.
      if (auto it = find_seed(x); it != seeds_.end())
        return key{hash(x, it->second)};
    }
    return caf::none;
  }

  /// Locates the digest for a given input.
  key find_digest(data_view x) const {
    auto i = find_seed(x);
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

  caf::expected<ids>
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
    if (op == equal || op == not_equal) {
      auto k = find_digest(x);
      auto eq = [=](const digest_type& digest) { return k == digest; };
      auto ne = [=](const digest_type& digest) { return k != digest; };
      return op == equal ? scan(eq) : scan(ne);
    }
    if (op == in || op == not_in) {
      // Ensure that the RHS is a list of strings.
      auto keys = caf::visit(
        detail::overload([&](auto xs) -> caf::expected<std::vector<key>> {
          using view_type = decltype(xs);
          if constexpr (detail::is_any_v<view_type, view<set>, view<vector>>) {
            std::vector<key> result;
            result.reserve(xs.size());
            for (auto x : xs)
              result.emplace_back(find_digest(x));
            return result;
          } else {
            return make_error(ec::type_clash, "expected set or vector on RHS",
                              materialize(x));
          }
        }),
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
      return op == in ? scan(in_pred) : scan(not_in_pred);
    }
    return make_error(ec::unsupported_operator, op);
  }

  bool immutable() const {
    return unique_digests_.empty() && !digests_.empty();
  }

  std::vector<digest_type> digests_;
  std::unordered_set<key, key_hasher> unique_digests_;

  // TODO: Once we can use C++20 and have a standard library that implements
  // key equivalcne properly, we can switch to std::unordered_map.
  detail::steady_map<data, size_t> seeds_;
};

} // namespace vast

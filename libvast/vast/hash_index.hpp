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
#include "vast/value_index.hpp"
#include "vast/view.hpp"

#include <caf/deserializer.hpp>
#include <caf/expected.hpp>
#include <caf/optional.hpp>
#include <caf/serializer.hpp>

#include <array>
#include <cstring>
#include <string>
#include <type_traits>
#include <unordered_map>
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
  static bool compare(digest_type x, digest_type y) {
    // The default equality operator may use element-wise comparison; this is
    // faster.
    return std::memcmp(x.data(), y.data(), x.size()) == 0;
  }

  struct key {
    friend bool operator==(key x, key y) {
      return compare(x.bytes, y.bytes);
    }

    friend bool operator!=(key x, key y) {
      return !(x == y);
    }

    friend bool operator==(key x, digest_type y) {
      return compare(x.bytes, y);
    }

    friend bool operator!=(key x, digest_type y) {
      return !(x == y);
    }

    friend bool operator==(digest_type x, key y) {
      return compare(x, y.bytes);
    }

    friend bool operator!=(digest_type x, key y) {
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
  caf::optional<key> make_digest(data_view x) {
    size_t i = 0;
    do {
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
      // FIXME: do not materialize every time, but this requires the ability to
      // use key equivalence (C++20).
      if (auto it = seeds_.find(materialize(x)); it != seeds_.end())
        return key{hash(x, it->second)};
      // Try the next hash function.
      ++i;
    } while (i < max_hash_rounds);
    return caf::none;
  }

  /// Locates the digest for a given input.
  key find_digest(data_view x) const {
    // FIXME: see above note on materialization.
    auto i = seeds_.find(materialize(x));
    return key{i != seeds_.end() ? hash(x, i->second) : hash(x, 0)};
  }

  bool append_impl(data_view x, id) override {
    auto digest = make_digest(x);
    if (!digest)
      return false;
    digests_.push_back(digest->bytes);
    return true;
  }

  caf::expected<ids>
  lookup_impl(relational_operator op, data_view x) const override {
    VAST_ASSERT(rank(this->mask()) == digests_.size());
    if (!(op == equal || op == not_equal))
      return make_error(ec::unsupported_operator, op);
    auto digest = find_digest(x);
    ewah_bitmap result;
    // Build candidate set of IDs.
    auto rng = select(this->mask());
    if (rng.done())
      return result;
    auto f = [&](auto predicate) {
      for (size_t i = 0, last_match = 0; i < digests_.size(); ++i) {
        if (predicate(digests_[i], digest)) {
          auto digests_since_last_match = i - last_match;
          if (digests_since_last_match > 0)
            rng.next(digests_since_last_match);
          result.append_bits(false, rng.get() - result.size());
          result.append_bit(true);
          last_match = i;
        }
      }
    };
    op == equal ? f(std::equal_to{}) : f(std::not_equal_to{});
    return result;
  }

  std::vector<digest_type> digests_;
  std::unordered_set<key, key_hasher> unique_digests_;
  std::unordered_map<data, size_t> seeds_;
};

} // namespace vast

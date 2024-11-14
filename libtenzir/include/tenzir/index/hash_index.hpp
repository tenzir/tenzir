//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/concepts.hpp"
#include "tenzir/data.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/legacy_deserialize.hpp"
#include "tenzir/detail/overload.hpp"
#include "tenzir/detail/stable_map.hpp"
#include "tenzir/detail/type_traits.hpp"
#include "tenzir/fbs/value_index.hpp"
#include "tenzir/hash/hash.hpp"
#include "tenzir/hash/legacy_hash.hpp"
#include "tenzir/logger.hpp"
#include "tenzir/operator.hpp"
#include "tenzir/value_index.hpp"
#include "tenzir/view.hpp"

#include <caf/binary_serializer.hpp>
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

namespace tenzir {

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
  // TODO: switch to XXH3 once the persistent index schema is versioned and
  // upgradable. Until then we have to support existing state produced by XXH64.
  using hash_algorithm = legacy_hash;
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
  explicit hash_index(tenzir::type t, caf::settings opts = {})
    : value_index{std::move(t), std::move(opts)} {
  }

  bool inspect_impl(supported_inspectors& inspector) override {
    return value_index::inspect_impl(inspector)
           && std::visit(
             [this]<class Inspector>(
               std::reference_wrapper<Inspector> visitor) {
               if constexpr (Inspector::is_loading) {
                 return this->deserialize(visitor.get());
               } else {
                 return this->serialize(visitor.get());
               }
             },
             inspector);
  }

  const std::vector<digest_type>& digests() const {
    return digests_;
  }

private:
  bool serialize(auto& serializer) {
    // Prune unneeded seeds.
    decltype(seeds_) non_null_seeds;
    for (auto& [k, v] : seeds_)
      if (v > 0)
        non_null_seeds.emplace(k, v);

    return serializer.apply(digests_) && serializer.apply(non_null_seeds);
  }

  bool deserialize(auto& deserializer) {
    return deserializer.apply(digests_) && deserializer.apply(seeds_);
  }

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
        TENZIR_ASSERT(result.second);
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
    TENZIR_ASSERT_EXPENSIVE(rank(this->mask()) == digests_.size());
    // Some operations we just cannot handle with this index, but they are still
    // valid operations. So for them we need to return all IDs.
    if (is<view<pattern>>(x)
        && (op == relational_operator::equal
            || op == relational_operator::not_equal)) {
      return ewah_bitmap{digests_.size(), true};
    }
    if (is<view<std::string>>(x)
        && (op == relational_operator::in || op == relational_operator::not_in
            || op == relational_operator::ni
            || op == relational_operator::not_ni)) {
      return ewah_bitmap{digests_.size(), true};
    }
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
      auto eq = [=](const digest_type& digest) {
        return k == digest;
      };
      auto ne = [=](const digest_type& digest) {
        return k != digest;
      };
      return op == relational_operator::equal ? scan(eq) : scan(ne);
    }
    if (op == relational_operator::in || op == relational_operator::not_in) {
      // Ensure that the RHS is a list of strings.
      auto keys = match(x, [&](auto xs) -> caf::expected<std::vector<key>> {
        using view_type = decltype(xs);
        if constexpr (std::is_same_v<view_type, view<list>>) {
          std::vector<key> result;
          result.reserve(xs.size());
          for (auto x : xs) {
            result.emplace_back(find_digest(x));
          }
          return result;
        } else {
          return caf::make_error(ec::type_clash, "expected list on RHS",
                                 materialize(x));
        }
      });
      if (!keys)
        return keys.error();
      // We're good to go with: create the set predicates an run the scan.
      auto in_pred = [&](const digest_type& digest) {
        auto cmp = [=](auto& k) {
          return k == digest;
        };
        return std::any_of(keys->begin(), keys->end(), cmp);
      };
      auto not_in_pred = [&](const digest_type& digest) {
        auto cmp = [=](auto& k) {
          return k == digest;
        };
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

  flatbuffers::Offset<fbs::ValueIndex>
  pack_impl(flatbuffers::FlatBufferBuilder& builder,
            flatbuffers::Offset<fbs::value_index::detail::ValueIndexBase>
              base_offset) override {
    auto digest_bytes = std::vector<uint8_t>{};
    digest_bytes.resize(digests_.size() * sizeof(digest_type));
    std::memcpy(digest_bytes.data(), digests_.data(), digest_bytes.size());
    auto unique_digest_bytes = std::vector<uint8_t>{};
    unique_digest_bytes.resize(unique_digests_.size() * sizeof(key));
    for (size_t i = 0; const auto& unique_digest : unique_digests_) {
      std::memcpy(unique_digest_bytes.data() + i * sizeof(key), &unique_digest,
                  sizeof(key));
      ++i;
    }
    auto seed_offsets = std::vector<
      flatbuffers::Offset<fbs::value_index::detail::HashIndexSeed>>{};
    seed_offsets.reserve(seeds_.size());
    for (const auto& [key, value] : seeds_) {
      const auto key_offset = pack(builder, key);
      seed_offsets.emplace_back(fbs::value_index::detail::CreateHashIndexSeed(
        builder, key_offset, value));
    }
    const auto hash_index_offset = fbs::value_index::CreateHashIndexDirect(
      builder, base_offset, &digest_bytes, &unique_digest_bytes, &seed_offsets);
    return fbs::CreateValueIndex(builder, fbs::value_index::ValueIndex::hash,
                                 hash_index_offset.Union());
  }

  caf::error unpack_impl(const fbs::ValueIndex& from) override {
    const auto* from_hash = from.value_index_as_hash();
    TENZIR_ASSERT(from_hash);
    TENZIR_ASSERT(from_hash->digests()->size() % sizeof(digest_type) == 0);
    const auto num_digests = from_hash->digests()->size() / sizeof(digest_type);
    digests_.reserve(num_digests);
    for (size_t i = 0; i < num_digests; ++i) {
      auto digest = digest_type{};
      std::memcpy(&digest,
                  from_hash->digests()->Data() + i * sizeof(digest_type),
                  sizeof(digest_type));
      digests_.emplace_back(digest);
    }
    TENZIR_ASSERT(from_hash->unique_digests()->size() % sizeof(key) == 0);
    const auto num_unique_digests
      = from_hash->unique_digests()->size() / sizeof(key);
    unique_digests_.reserve(num_unique_digests);
    for (size_t i = 0; i < num_digests; ++i) {
      auto digest = key{};
      std::memcpy(&digest,
                  from_hash->unique_digests()->Data() + i * sizeof(key),
                  sizeof(key));
      unique_digests_.insert(digest);
    }
    seeds_.reserve(from_hash->seeds()->size());
    for (const auto& seed : *from_hash->seeds()) {
      auto key = data{};
      if (auto err = unpack(*seed->key(), key))
        return err;
      auto ok = seeds_.emplace(key, seed->value()).second;
      TENZIR_ASSERT(ok);
    }
    return caf::none;
  }

  [[nodiscard]] bool immutable() const {
    return unique_digests_.empty() && !digests_.empty();
  }

  std::vector<digest_type> digests_;
  std::unordered_set<key, key_hasher> unique_digests_;

  // We use a robin_map here because it supports heterogeneous lookup, which
  // has a major performance impact for `seeds_`, see ch13760.
  using seeds_map = tsl::robin_map<data, size_t>;
  static_assert(concepts::transparent<seeds_map::key_equal>);
  seeds_map seeds_;
};

} // namespace tenzir

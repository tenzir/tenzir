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

#include "vast/bloom_filter_parameters.hpp"
#include "vast/bloom_filter_synopsis.hpp"
#include "vast/fwd.hpp"

#include <caf/config_value.hpp>
#include <caf/settings.hpp>

#include <vast/detail/assert.hpp>
#include <vast/error.hpp>
#include <vast/logger.hpp>

namespace vast {

template <class HashFunction>
synopsis_ptr
make_string_synopsis(vast::type type, bloom_filter_parameters params,
                     std::vector<size_t> seeds = {});

/// A synopsis for strings.
template <class HashFunction>
class string_synopsis final
  : public bloom_filter_synopsis<std::string, HashFunction> {
public:
  using super = bloom_filter_synopsis<std::string, HashFunction>;

  /// Constructs a string synopsis from an `string_type` and a Bloom
  /// filter.
  string_synopsis(type x, typename super::bloom_filter_type bf)
    : super{std::move(x), std::move(bf)} {
    VAST_ASSERT(caf::holds_alternative<string_type>(this->type()));
  }

  bool equals(const synopsis& other) const noexcept override {
    if (typeid(other) != typeid(string_synopsis))
      return false;
    auto& rhs = static_cast<const string_synopsis&>(other);
    return this->type() == rhs.type()
           && this->bloom_filter_ == rhs.bloom_filter_;
  }
};

/// A synopsis for strings that stores a full copy of the input in a hash
/// table to be able to construct a smaller bloom filter synopsis for this data
/// at a later point in time using the `shrink` function.
template <class HashFunction>
class buffered_string_synopsis : public synopsis {
public:
  buffered_string_synopsis(vast::type x, double p)
    : synopsis{std::move(x)}, p_{p} {
  }

  synopsis_ptr shrink() const override {
    size_t next_power_of_two = 1ull;
    while (strings_.size() > next_power_of_two)
      next_power_of_two *= 2;
    bloom_filter_parameters params;
    params.p = p_;
    params.n = next_power_of_two;
    VAST_DEBUG_ANON("shrinked string synopsis to", params.n, "elements");
    auto& type = this->type();
    auto shrinked_synopsis
      = make_string_synopsis<xxhash64>(type, std::move(params));
    if (!shrinked_synopsis)
      return nullptr;
    for (auto& s : strings_)
      shrinked_synopsis->add(make_view(s));
    return shrinked_synopsis;
  }

  // Implementation of the remainder of the `synopsis` API.
  void add(data_view x) override {
    auto sv = caf::get_if<view<std::string>>(&x);
    VAST_ASSERT(sv);
    strings_.insert(materialize(*sv));
  }

  size_t size_bytes() const override {
    return sizeof(buffered_string_synopsis)
           + strings_.size() * sizeof(typename decltype(strings_)::node_type);
  }

  caf::optional<bool>
  lookup(relational_operator op, data_view rhs) const override {
    switch (op) {
      default:
        return caf::none;
      case equal:
        return strings_.count(materialize(caf::get<std::string_view>(rhs)));
      case in: {
        if (auto xs = caf::get_if<view<list>>(&rhs)) {
          for (auto x : **xs)
            if (strings_.count(materialize(caf::get<std::string_view>(x))))
              return true;
          return false;
        }
        return caf::none;
      }
    }
  }

  caf::error serialize(caf::serializer&) const override {
    return make_error(ec::logic_error, "attempted to serialize a "
                                       "buffered_string_synopsis; did you "
                                       "forget to shrink?");
  }

  caf::error deserialize(caf::deserializer&) override {
    return make_error(ec::logic_error, "attempted to deserialize a "
                                       "buffered_string_synopsis");
  }

  bool equals(const synopsis& other) const noexcept override {
    if (auto* p = dynamic_cast<const buffered_string_synopsis*>(&other))
      return strings_ == p->strings_;
    return false;
  }

private:
  double p_;
  std::unordered_set<std::string> strings_;
};

/// Factory to construct a string synopsis.
/// @tparam HashFunction The hash function to use for the Bloom filter.
/// @param type A type instance carrying an `string_type`.
/// @param params The Bloom filter parameters.
/// @param seeds The seeds for the Bloom filter hasher.
/// @returns A type-erased pointer to a synopsis.
/// @pre `caf::holds_alternative<string_type>(type)`.
/// @relates string_synopsis
template <class HashFunction>
synopsis_ptr
make_string_synopsis(vast::type type, bloom_filter_parameters params,
                     std::vector<size_t> seeds) {
  VAST_ASSERT(caf::holds_alternative<string_type>(type));
  auto x = make_bloom_filter<HashFunction>(std::move(params), std::move(seeds));
  if (!x) {
    VAST_WARNING_ANON(__func__, "failed to construct Bloom filter");
    return nullptr;
  }
  using synopsis_type = string_synopsis<HashFunction>;
  return std::make_unique<synopsis_type>(std::move(type), std::move(*x));
}

/// Factory to construct a buffered string synopsis.
/// @tparam HashFunction The hash function to use for the Bloom filter.
/// @param type A type instance carrying an `string_type`.
/// @param params The Bloom filter parameters.
/// @param seeds The seeds for the Bloom filter hasher.
/// @returns A type-erased pointer to a synopsis.
/// @pre `caf::holds_alternative<string_type>(type)`.
/// @relates string_synopsis
template <class HashFunction>
synopsis_ptr
make_buffered_string_synopsis(vast::type type, bloom_filter_parameters params) {
  VAST_ASSERT(caf::holds_alternative<string_type>(type));
  if (!params.p) {
    return nullptr;
  }
  using synopsis_type = buffered_string_synopsis<HashFunction>;
  return std::make_unique<synopsis_type>(std::move(type), *params.p);
}

/// Factory to construct a string synopsis. This overload looks for a type
/// attribute containing the Bloom filter parameters and hash function seeds.
/// @tparam HashFunction The hash function to use for the Bloom filter.
/// @param type A type instance carrying an `string_type`.
/// @returns A type-erased pointer to a synopsis.
/// @relates string_synopsis
template <class HashFunction>
synopsis_ptr make_string_synopsis(vast::type type, const caf::settings& opts) {
  VAST_ASSERT(caf::holds_alternative<string_type>(type));
  if (auto xs = parse_parameters(type))
    return make_string_synopsis<HashFunction>(std::move(type), std::move(*xs));
  // If no explicit Bloom filter parameters were attached to the type, we try
  // to use the maximum partition size of the index as upper bound for the
  // expected number of events.
  using int_type = caf::config_value::integer;
  auto max_part_size = caf::get_if<int_type>(&opts, "max-partition-size");
  if (!max_part_size) {
    VAST_ERROR_ANON(__func__, "could not determine Bloom filter parameters");
    return nullptr;
  }
  bloom_filter_parameters params;
  params.n = *max_part_size;
  params.p = defaults::system::string_synopsis_fprate;
  // Because VAST deserializes a synopsis with empty options and
  // construction of an string synopsis fails without any sizing
  // information, we augment the type with the synopsis options.
  using namespace std::string_literals;
  auto v = "bloomfilter("s + std::to_string(*params.n) + ','
           + std::to_string(*params.p) + ')';
  auto t = type.attributes({{"synopsis", std::move(v)}});
  // Create either a a buffered_string_synopsis or a plain string synopsis
  // depending on the callers preference.
  auto buffered = caf::get_or(opts, "buffer-ips", false);
  auto result
    = buffered
        ? make_buffered_string_synopsis<HashFunction>(std::move(t), params)
        : make_string_synopsis<HashFunction>(std::move(t), params);
  if (!result)
    VAST_ERROR_ANON(__func__,
                    "failed to evaluate Bloom filter parameters:", params.n,
                    params.p);
  return result;
}

} // namespace vast

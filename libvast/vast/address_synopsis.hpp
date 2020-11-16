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

#include <vast/address.hpp>
#include <vast/detail/assert.hpp>
#include <vast/logger.hpp>

namespace vast {

template <class HashFunction>
synopsis_ptr
make_address_synopsis(vast::type type, bloom_filter_parameters params,
                      std::vector<size_t> seeds = {});

/// A synopsis for IP addresses.
template <class HashFunction>
class address_synopsis final
  : public bloom_filter_synopsis<address, HashFunction> {
public:
  using super = bloom_filter_synopsis<address, HashFunction>;

  /// Constructs an IP address synopsis from an `address_type` and a Bloom
  /// filter.
  address_synopsis(type x, typename super::bloom_filter_type bf)
    : super{std::move(x), std::move(bf)} {
    VAST_ASSERT(caf::holds_alternative<address_type>(this->type()));
  }

  bool equals(const synopsis& other) const noexcept override {
    if (typeid(other) != typeid(address_synopsis))
      return false;
    auto& rhs = static_cast<const address_synopsis&>(other);
    return this->type() == rhs.type()
           && this->bloom_filter_ == rhs.bloom_filter_;
  }
};

/// A synopsis for IP addresses that stores a copy of the input to
/// be able to construct a smaller bloom filter from this data at
/// some point using the `shrink` function.
template <class HashFunction>
class buffered_address_synopsis
  : public bloom_filter_synopsis<address, HashFunction> {
public:
  using super = bloom_filter_synopsis<address, HashFunction>;

  buffered_address_synopsis(type x, typename super::bloom_filter_type bf)
    : super{std::move(x), std::move(bf)} {
  }

  void add(data_view x) override {
    auto addr_view = caf::get_if<view<address>>(&x);
    VAST_ASSERT(addr_view);
    ips_.push_back(*addr_view);
    super::add(x);
  }

  synopsis_ptr shrink() override {
    // The bloom_filter doesnt store its `p`, so we parse the type to get it,
    // which was enriched with this information in `make_address_synopsis()`.
    auto& type = this->type();
    auto old_params = parse_parameters(type);
    if (!old_params)
      return nullptr;
    std::sort(ips_.begin(), ips_.end());
    auto end = std::unique(ips_.begin(), ips_.end());
    auto begin = ips_.begin();
    auto n = std::distance(begin, end);
    ips_.resize(n);
    size_t next_power_of_two = 1ull;
    while (ips_.size() > next_power_of_two)
      next_power_of_two *= 2;
    bloom_filter_parameters params;
    params.p = old_params->p;
    params.n = next_power_of_two;
    VAST_DEBUG_ANON("shrinked address synopsis from", old_params->n, "to", n);
    auto shrinked_synopsis
      = make_address_synopsis<xxhash64>(type, std::move(params));
    if (!shrinked_synopsis)
      return nullptr;
    for (auto it = begin; it != end; ++it)
      shrinked_synopsis->add(*it);
    return shrinked_synopsis;
  }

  bool equals(const synopsis& other) const noexcept override {
    if (typeid(other) != typeid(buffered_address_synopsis))
      return false;
    auto& rhs = static_cast<const buffered_address_synopsis&>(other);
    return this->type() == rhs.type()
           && this->bloom_filter_ == rhs.bloom_filter_;
  }

private:
  std::vector<vast::address> ips_;
};

/// Factory to construct an IP address synopsis.
/// @tparam HashFunction The hash function to use for the Bloom filter.
/// @param type A type instance carrying an `address_type`.
/// @param params The Bloom filter parameters.
/// @param seeds The seeds for the Bloom filter hasher.
/// @returns A type-erased pointer to a synopsis.
/// @pre `caf::holds_alternative<address_type>(type)`.
/// @relates address_synopsis
template <class HashFunction>
synopsis_ptr
make_address_synopsis(vast::type type, bloom_filter_parameters params,
                      std::vector<size_t> seeds) {
  VAST_ASSERT(caf::holds_alternative<address_type>(type));
  auto x = make_bloom_filter<HashFunction>(std::move(params), std::move(seeds));
  if (!x) {
    VAST_WARNING_ANON(__func__, "failed to construct Bloom filter");
    return nullptr;
  }
  using synopsis_type = address_synopsis<HashFunction>;
  return std::make_unique<synopsis_type>(std::move(type), std::move(*x));
}

/// Factory to construct a buffered IP address synopsis.
/// @tparam HashFunction The hash function to use for the Bloom filter.
/// @param type A type instance carrying an `address_type`.
/// @param params The Bloom filter parameters.
/// @param seeds The seeds for the Bloom filter hasher.
/// @returns A type-erased pointer to a synopsis.
/// @pre `caf::holds_alternative<address_type>(type)`.
/// @relates address_synopsis
template <class HashFunction>
synopsis_ptr
make_buffered_address_synopsis(vast::type type, bloom_filter_parameters params,
                               std::vector<size_t> seeds = {}) {
  VAST_ASSERT(caf::holds_alternative<address_type>(type));
  auto x = make_bloom_filter<HashFunction>(std::move(params), std::move(seeds));
  if (!x) {
    VAST_WARNING_ANON(__func__, "failed to construct Bloom filter");
    return nullptr;
  }
  using synopsis_type = buffered_address_synopsis<HashFunction>;
  return std::make_unique<synopsis_type>(std::move(type), std::move(*x));
}

/// Factory to construct an IP address synopsis. This overload looks for a type
/// attribute containing the Bloom filter parameters and hash function seeds.
/// @tparam HashFunction The hash function to use for the Bloom filter.
/// @param type A type instance carrying an `address_type`.
/// @returns A type-erased pointer to a synopsis.
/// @relates address_synopsis
template <class HashFunction>
synopsis_ptr make_address_synopsis(vast::type type, const caf::settings& opts) {
  VAST_ASSERT(caf::holds_alternative<address_type>(type));
  if (auto xs = parse_parameters(type))
    return make_address_synopsis<HashFunction>(std::move(type), std::move(*xs));
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
  params.p = defaults::system::address_synopsis_fprate;
  // Because VAST deserializes a synopsis with empty options and
  // construction of an address synopsis fails without any sizing
  // information, we augment the type with the synopsis options.
  using namespace std::string_literals;
  auto v = "bloomfilter("s + std::to_string(*params.n) + ','
           + std::to_string(*params.p) + ')';
  auto t = type.attributes({{"synopsis", std::move(v)}});
  // Create either a a buffered_address_synopsis or a plain address synopsis
  // depending on the callers preference.
  auto buffered = caf::get_or(opts, "buffer-ips", false);
  VAST_WARNING_ANON("options are", opts, "so we are", buffered);
  auto result
    = buffered
        ? make_buffered_address_synopsis<HashFunction>(std::move(t), params)
        : make_address_synopsis<HashFunction>(std::move(t), params);
  if (!result)
    VAST_ERROR_ANON(__func__,
                    "failed to evaluate Bloom filter parameters:", params.n,
                    params.p);
  return result;
}

} // namespace vast

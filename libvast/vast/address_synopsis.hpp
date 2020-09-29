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

#include <caf/config_value.hpp>
#include <caf/settings.hpp>

#include <vast/address.hpp>
#include <vast/detail/assert.hpp>
#include <vast/logger.hpp>

namespace vast {

/// A synopsis for IP addresses.
template <class HashFunction>
class address_synopsis final
  : public bloom_filter_synopsis<address, HashFunction> {
public:
  using super = bloom_filter_synopsis<address, HashFunction>;

  /// Concstructs an IP address synopsis from an `address_type` and a Bloom
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
                      std::vector<size_t> seeds = {}) {
  VAST_ASSERT(caf::holds_alternative<address_type>(type));
  auto x = make_bloom_filter<HashFunction>(std::move(params), std::move(seeds));
  if (!x) {
    VAST_WARNING_ANON(__func__, "failed to construct Bloom filter");
    return nullptr;
  }
  using synopsis_type = address_synopsis<HashFunction>;
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
  auto make = [](auto x, auto xs) {
    return make_address_synopsis<HashFunction>(std::move(x), std::move(xs));
  };
  if (auto xs = parse_parameters(type))
    return make(std::move(type), std::move(*xs));
  // If no explicit Bloom filter parameters were attached to the type, we try
  // to use the maximum partition size of the index as upper bound for the
  // expected number of events.
  using int_type = caf::config_value::integer;
  if (auto max_part_size = caf::get_if<int_type>(&opts, "max-partition-size")) {
    bloom_filter_parameters xs;
    xs.n = *max_part_size;
    xs.p = 0.01;
    // Because VAST deserializes a synopsis with empty options and
    // construction of an address synopsis fails without any sizing
    // information, we augment the type with the synopsis options.
    using namespace std::string_literals;
    auto v = "bloomfilter("s + std::to_string(*xs.n) + ','
             + std::to_string(*xs.p) + ')';
    auto t = type.attributes({{"synopsis", std::move(v)}});
    auto result = make(std::move(t), std::move(xs));
    if (!result)
      VAST_ERROR_ANON(
        __func__, "failed to evaluate Bloom filter parameters:", xs.n, xs.p);
    return result;
  }
  VAST_ERROR_ANON(__func__, "could not determine Bloom filter parameters");
  return nullptr;
}

} // namespace vast

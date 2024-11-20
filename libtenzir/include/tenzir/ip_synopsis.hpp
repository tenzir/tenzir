//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include "tenzir/bloom_filter_parameters.hpp"
#include "tenzir/bloom_filter_synopsis.hpp"
#include "tenzir/buffered_synopsis.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/error.hpp"
#include "tenzir/ip.hpp"
#include "tenzir/logger.hpp"

#include <caf/config_value.hpp>
#include <caf/settings.hpp>

namespace tenzir {

template <class HashFunction>
synopsis_ptr make_ip_synopsis(tenzir::type type, bloom_filter_parameters params,
                              std::vector<size_t> seeds = {});

/// A synopsis for IP addresses.
template <class HashFunction>
class ip_synopsis final : public bloom_filter_synopsis<ip, HashFunction> {
public:
  using super = bloom_filter_synopsis<ip, HashFunction>;

  /// Constructs an IP address synopsis from an `ip_type` and a
  /// Bloom filter.
  ip_synopsis(type x, typename super::bloom_filter_type bf)
    : super{std::move(x), std::move(bf)} {
    TENZIR_ASSERT(is<ip_type>(this->type()));
  }

  [[nodiscard]] synopsis_ptr clone() const override {
    return this->super::clone();
  }

  [[nodiscard]] bool equals(const synopsis& other) const noexcept override {
    if (typeid(other) != typeid(ip_synopsis))
      return false;
    auto& rhs = static_cast<const ip_synopsis&>(other);
    return this->type() == rhs.type()
           && this->bloom_filter_ == rhs.bloom_filter_;
  }
};

/// @relates buffered_synopsis_traits
template <>
struct buffered_synopsis_traits<tenzir::ip> {
  template <typename HashFunction>
  static synopsis_ptr make(tenzir::type type, bloom_filter_parameters p,
                           std::vector<size_t> seeds = {}) {
    return make_ip_synopsis<HashFunction>(std::move(type), std::move(p),
                                          std::move(seeds));
  }

  // Estimate the size in bytes for an unordered_set of T.
  static size_t memusage(const std::unordered_set<tenzir::ip>& x) {
    using node_type = typename std::decay_t<decltype(x)>::node_type;
    return x.size() * sizeof(node_type);
  }
};

template <typename HashFunction>
using buffered_ip_synopsis = buffered_synopsis<tenzir::ip, HashFunction>;

/// Factory to construct an IP address synopsis.
/// @tparam HashFunction The hash function to use for the Bloom filter.
/// @param type A type instance carrying an `ip_type`.
/// @param params The Bloom filter parameters.
/// @param seeds The seeds for the Bloom filter hasher.
/// @returns A type-erased pointer to a synopsis.
/// @pre `is<ip_type>(type)`.
/// @relates ip_synopsis
template <class HashFunction>
synopsis_ptr make_ip_synopsis(tenzir::type type, bloom_filter_parameters params,
                              std::vector<size_t> seeds) {
  TENZIR_ASSERT(is<ip_type>(type));
  auto x = make_bloom_filter<HashFunction>(params, std::move(seeds));
  if (!x) {
    TENZIR_WARN("{} failed to construct Bloom filter", __func__);
    return nullptr;
  }
  using synopsis_type = ip_synopsis<HashFunction>;
  return std::make_unique<synopsis_type>(std::move(type), std::move(*x));
}

/// Factory to construct a buffered IP address synopsis.
/// @tparam HashFunction The hash function to use for the Bloom filter.
/// @param type A type instance carrying an `ip_type`.
/// @param params The Bloom filter parameters.
/// @param seeds The seeds for the Bloom filter hasher.
/// @returns A type-erased pointer to a synopsis.
/// @pre `is<ip_type>(type)`.
/// @relates ip_synopsis
template <class HashFunction>
synopsis_ptr
make_buffered_ip_synopsis(tenzir::type type, bloom_filter_parameters params) {
  TENZIR_ASSERT(is<ip_type>(type));
  if (!params.p) {
    return nullptr;
  }
  using synopsis_type = buffered_ip_synopsis<HashFunction>;
  return std::make_unique<synopsis_type>(std::move(type), *params.p);
}

/// Factory to construct an IP address synopsis. This overload looks for a type
/// attribute containing the Bloom filter parameters and hash function seeds.
/// @tparam HashFunction The hash function to use for the Bloom filter.
/// @param type A type instance carrying an `ip_type`.
/// @returns A type-erased pointer to a synopsis.
/// @relates ip_synopsis
template <class HashFunction>
synopsis_ptr make_ip_synopsis(tenzir::type type, const caf::settings& opts) {
  TENZIR_ASSERT(is<ip_type>(type));
  if (auto xs = parse_parameters(type))
    return make_ip_synopsis<HashFunction>(std::move(type), *xs);
  // If no explicit Bloom filter parameters were attached to the type, we try
  // to use the maximum partition size of the index as upper bound for the
  // expected number of events.
  using int_type = caf::config_value::integer;
  auto max_part_size = caf::get_if<int_type>(&opts, "max-partition-size");
  if (!max_part_size) {
    TENZIR_ERROR("{} could not determine Bloom filter parameters",
                 __PRETTY_FUNCTION__);
    return nullptr;
  }
  bloom_filter_parameters params;
  params.n = *max_part_size;
  params.p = caf::get_or(opts, "address-synopsis-fp-rate", defaults::fp_rate);
  auto annotated_type = annotate_parameters(type, params);
  // Create either a a buffered_ip_synopsis or a plain address synopsis
  // depending on the callers preference.
  auto buffered = caf::get_or(opts, "buffer-input-data", false);
  auto result
    = buffered
        ? make_buffered_ip_synopsis<HashFunction>(std::move(type), params)
        : make_ip_synopsis<HashFunction>(std::move(annotated_type), params);
  if (!result)
    TENZIR_ERROR("{} failed to evaluate Bloom filter parameters: {} {}",
                 __func__, params.n, params.p);
  return result;
}

} // namespace tenzir

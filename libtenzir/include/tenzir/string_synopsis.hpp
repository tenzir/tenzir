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
#include "tenzir/logger.hpp"

#include <caf/config_value.hpp>
#include <caf/settings.hpp>

namespace tenzir {

template <class HashFunction>
synopsis_ptr
make_string_synopsis(tenzir::type type, bloom_filter_parameters params,
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
    TENZIR_ASSERT(is<string_type>(this->type()));
  }

  [[nodiscard]] synopsis_ptr clone() const override {
    return this->super::clone();
  }

  [[nodiscard]] bool equals(const synopsis& other) const noexcept override {
    if (typeid(other) != typeid(string_synopsis))
      return false;
    auto& rhs = static_cast<const string_synopsis&>(other);
    return this->type() == rhs.type()
           && this->bloom_filter_ == rhs.bloom_filter_;
  }
};

/// @relates buffered_synopsis_traits
template <>
struct buffered_synopsis_traits<std::string> {
  template <typename HashFunction>
  static synopsis_ptr make(tenzir::type type, bloom_filter_parameters p,
                           std::vector<size_t> seeds = {}) {
    return make_string_synopsis<HashFunction>(std::move(type), std::move(p),
                                              std::move(seeds));
  }

  static size_t memusage(const std::unordered_set<std::string>& x) {
    using node_type = typename std::decay_t<decltype(x)>::node_type;
    size_t result = 0;
    for (auto& s : x)
      result += sizeof(node_type) + s.size();
    return result;
  }
};

template <typename Hash>
using buffered_string_synopsis = buffered_synopsis<std::string, Hash>;

/// Factory to construct a string synopsis.
/// @tparam HashFunction The hash function to use for the Bloom filter.
/// @param type A type instance carrying an `string_type`.
/// @param params The Bloom filter parameters.
/// @param seeds The seeds for the Bloom filter hasher.
/// @returns A type-erased pointer to a synopsis.
/// @pre `is<string_type>(type)`.
/// @relates string_synopsis
template <class HashFunction>
synopsis_ptr
make_string_synopsis(tenzir::type type, bloom_filter_parameters params,
                     std::vector<size_t> seeds) {
  TENZIR_ASSERT(is<string_type>(type));
  auto x = make_bloom_filter<HashFunction>(std::move(params), std::move(seeds));
  if (!x) {
    TENZIR_WARN("{} failed to construct Bloom filter", __func__);
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
/// @pre `is<string_type>(type)`.
/// @relates string_synopsis
template <class HashFunction>
synopsis_ptr make_buffered_string_synopsis(tenzir::type type,
                                           bloom_filter_parameters params) {
  TENZIR_ASSERT(is<string_type>(type));
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
synopsis_ptr
make_string_synopsis(tenzir::type type, const caf::settings& opts) {
  TENZIR_ASSERT(is<string_type>(type));
  if (auto xs = parse_parameters(type))
    return make_string_synopsis<HashFunction>(std::move(type), std::move(*xs));
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
  params.p = caf::get_or(opts, "string-synopsis-fp-rate", defaults::fp_rate);
  auto annotated_type = annotate_parameters(type, params);
  // Create either a a buffered_string_synopsis or a plain string synopsis
  // depending on the callers preference.
  auto buffered = caf::get_or(opts, "buffer-input-data", false);
  auto result
    = buffered
        ? make_buffered_string_synopsis<HashFunction>(std::move(type), params)
        : make_string_synopsis<HashFunction>(std::move(annotated_type), params);
  if (!result)
    TENZIR_ERROR("{} failed to evaluate Bloom filter parameters: {} {}",
                 __func__, params.n, params.p);
  return result;
}

} // namespace tenzir

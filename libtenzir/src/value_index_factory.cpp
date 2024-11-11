//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/value_index_factory.hpp"

#include "tenzir/base.hpp"
#include "tenzir/concept/parseable/numeric/integral.hpp"
#include "tenzir/concept/parseable/tenzir/base.hpp"
#include "tenzir/detail/type_traits.hpp"
#include "tenzir/index/arithmetic_index.hpp"
#include "tenzir/index/enumeration_index.hpp"
#include "tenzir/index/hash_index.hpp"
#include "tenzir/index/ip_index.hpp"
#include "tenzir/index/list_index.hpp"
#include "tenzir/index/string_index.hpp"
#include "tenzir/index/subnet_index.hpp"
#include "tenzir/logger.hpp"
#include "tenzir/type.hpp"
#include "tenzir/value_index.hpp"

#include <caf/optional.hpp>
#include <caf/settings.hpp>

#include <bit>
#include <cmath>

using namespace std::string_view_literals;

namespace tenzir {
namespace {

template <class T>
value_index_ptr make(type x, caf::settings opts) {
  using int_type = caf::config_value::integer;
  // The cardinality must be an integer.
  if (auto i = opts.find("cardinality"); i != opts.end()) {
    if (!caf::holds_alternative<int_type>(i->second)) {
      TENZIR_ERROR("{} invalid cardinality type", __func__);
      return nullptr;
    }
  }
  // The base specification has its own grammar.
  if (auto i = opts.find("base"); i != opts.end()) {
    auto str = try_as<caf::config_value::string>(&i->second);
    if (!str) {
      TENZIR_ERROR("{} invalid base type (string type needed)", __func__);
      return nullptr;
    }
    if (!parsers::base(*str)) {
      TENZIR_ERROR("{} invalid base specification", __func__);
      return nullptr;
    }
  }
  if (auto index = x.attribute("index")) {
    if (*index == "hash"sv) {
      auto i = opts.find("cardinality");
      if (i == opts.end())
        // Default to a 40-bit hash value -> good for 2^20 unique digests.
        return std::make_unique<hash_index<5>>(std::move(x));
      const auto* cardinality_option = try_as<int_type>(&i->second);
      TENZIR_ASSERT(cardinality_option); // checked in make(x, opts)
      // caf::settings doesn't support unsigned integers, but the
      // cardinality is a size_t, so we may get negative values if someone
      // provides an uint64_t value, e.g., numeric_limits<size_t>::max().
      if (*cardinality_option < 0) {
        TENZIR_WARN("{} got an explicit cardinality of 2^64, using "
                    "max digest size of 8 bytes",
                    __func__);
        return std::make_unique<hash_index<8>>(std::move(x));
      }
      // Need an unsigned value for bit-level operations below.
      auto cardinality
        = static_cast<std::make_unsigned_t<int_type>>(*cardinality_option);
      if (!std::has_single_bit(cardinality))
        TENZIR_WARN("{} cardinality not a power of 2", __func__);
      // For 2^n unique values, we expect collisions after sqrt(2^n).
      // Thus, we use 2n bits as digest size.
      size_t digest_bits = std::has_single_bit(cardinality)
                             ? (std::bit_width(cardinality) - 1) * 2
                             : std::bit_width(cardinality) * 2;
      auto digest_bytes = digest_bits / 8;
      if (digest_bits % 8 > 0)
        ++digest_bytes;
      TENZIR_DEBUG("{} creating hash index with a digest of {} bytes", __func__,
                   digest_bytes);
      if (digest_bytes > 8) {
        TENZIR_WARN("{} expected cardinality exceeds "
                    "maximum digest size, capping at 8 bytes",
                    __func__);
        digest_bytes = 8;
      }
      switch (digest_bytes) {
        default:
          TENZIR_ERROR("{} invalid digest size {}", __func__, cardinality);
          return nullptr;
        case 1:
          return std::make_unique<hash_index<1>>(std::move(x), std::move(opts));
        case 2:
          return std::make_unique<hash_index<2>>(std::move(x), std::move(opts));
        case 3:
          return std::make_unique<hash_index<3>>(std::move(x), std::move(opts));
        case 4:
          return std::make_unique<hash_index<4>>(std::move(x), std::move(opts));
        case 5:
          return std::make_unique<hash_index<5>>(std::move(x), std::move(opts));
        case 6:
          return std::make_unique<hash_index<6>>(std::move(x), std::move(opts));
        case 7:
          return std::make_unique<hash_index<7>>(std::move(x), std::move(opts));
        case 8:
          return std::make_unique<hash_index<8>>(std::move(x), std::move(opts));
      }
    }
  }
  return std::make_unique<T>(std::move(x), std::move(opts));
}

template <concrete_type T, class Index>
auto add_value_index_factory(T&& x = {}) {
  return factory<value_index>::add(type{std::forward<T>(x)}, make<Index>);
}

template <class T>
auto add_arithmetic_index_factory() {
  static_assert(detail::is_any_v<T, int64_type, uint64_type, enumeration_type,
                                 double_type, duration_type, time_type>);
  using concrete_data = type_to_data_t<T>;
  return add_value_index_factory<T, arithmetic_index<concrete_data>>();
}

} // namespace

void factory_traits<value_index>::initialize() {
  add_value_index_factory<bool_type, arithmetic_index<bool>>();
  add_arithmetic_index_factory<int64_type>();
  add_arithmetic_index_factory<uint64_type>();
  add_arithmetic_index_factory<double_type>();
  add_arithmetic_index_factory<duration_type>();
  add_arithmetic_index_factory<time_type>();
  add_value_index_factory<ip_type, ip_index>();
  add_value_index_factory<subnet_type, subnet_index>();
  add_value_index_factory<string_type, string_index>();
  // List and enumeration types are not default-constructible, but their
  // contents dont matter here. We should refactor this at some point to just
  // need a template type.
  add_value_index_factory<enumeration_type, enumeration_index>(
    enumeration_type{{"stub"}});
  add_value_index_factory<list_type, list_index>(list_type{type{}});
}

factory_traits<value_index>::key_type
factory_traits<value_index>::key(const type& t) {
  return t.type_index();
}

} // namespace tenzir

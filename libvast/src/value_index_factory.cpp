//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/value_index_factory.hpp"

#include "vast/base.hpp"
#include "vast/concept/parseable/numeric/integral.hpp"
#include "vast/concept/parseable/vast/base.hpp"
#include "vast/detail/bit.hpp"
#include "vast/detail/type_traits.hpp"
#include "vast/index/address_index.hpp"
#include "vast/index/arithmetic_index.hpp"
#include "vast/index/enumeration_index.hpp"
#include "vast/index/hash_index.hpp"
#include "vast/index/list_index.hpp"
#include "vast/index/string_index.hpp"
#include "vast/index/subnet_index.hpp"
#include "vast/legacy_type.hpp"
#include "vast/logger.hpp"
#include "vast/value_index.hpp"

#include <caf/optional.hpp>
#include <caf/settings.hpp>

#include <cmath>

using namespace std::string_view_literals;

namespace vast {
namespace {

template <class T>
value_index_ptr make(legacy_type x, caf::settings opts) {
  using int_type = caf::config_value::integer;
  // The cardinality must be an integer.
  if (auto i = opts.find("cardinality"); i != opts.end()) {
    if (!caf::holds_alternative<int_type>(i->second)) {
      VAST_ERROR("{} invalid cardinality type", __func__);
      return nullptr;
    }
  }
  // The base specification has its own grammar.
  if (auto i = opts.find("base"); i != opts.end()) {
    auto str = caf::get_if<caf::config_value::string>(&i->second);
    if (!str) {
      VAST_ERROR("{} invalid base type (string type needed)", __func__);
      return nullptr;
    }
    if (!parsers::base(*str)) {
      VAST_ERROR("{} invalid base specification", __func__);
      return nullptr;
    }
  }
  if (auto a = find_attribute(x, "index")) {
    if (auto value = a->value)
      if (*value == "hash"sv) {
        auto i = opts.find("cardinality");
        if (i == opts.end())
          // Default to a 40-bit hash value -> good for 2^20 unique digests.
          return std::make_unique<hash_index<5>>(std::move(x));
        auto cardinality = caf::get_if<int_type>(&i->second);
        VAST_ASSERT(cardinality); // checked in make(x, opts)
        // caf::settings doesn't support unsigned integers, but the
        // cardinality is a size_t, so we may get negative values if someone
        // provides an uint64_t value, e.g., numeric_limits<size_t>::max().
        if (*cardinality < 0) {
          VAST_WARN("{} got an explicit cardinality of 2^64, using "
                    "max digest size of 8 bytes",
                    __func__);
          return std::make_unique<hash_index<8>>(std::move(x));
        }
        if (!detail::has_single_bit(*cardinality))
          VAST_WARN("{} cardinality not a power of 2", __func__);
        // For 2^n unique values, we expect collisions after sqrt(2^n).
        // Thus, we use 2n bits as digest size.
        size_t digest_bits = detail::has_single_bit(*cardinality)
                               ? (detail::bit_width(*cardinality) - 1) * 2
                               : detail::bit_width(*cardinality) * 2;
        auto digest_bytes = digest_bits / 8;
        if (digest_bits % 8 > 0)
          ++digest_bytes;
        VAST_DEBUG("{} creating hash index with a digest of {} bytes", __func__,
                   digest_bytes);
        if (digest_bytes > 8) {
          VAST_WARN("{} expected cardinality exceeds "
                    "maximum digest size, capping at 8 bytes",
                    __func__);
          digest_bytes = 8;
        }
        switch (digest_bytes) {
          default:
            VAST_ERROR("{} invalid digest size {}", __func__, *cardinality);
            return nullptr;
          case 1:
            return std::make_unique<hash_index<1>>(std::move(x),
                                                   std::move(opts));
          case 2:
            return std::make_unique<hash_index<2>>(std::move(x),
                                                   std::move(opts));
          case 3:
            return std::make_unique<hash_index<3>>(std::move(x),
                                                   std::move(opts));
          case 4:
            return std::make_unique<hash_index<4>>(std::move(x),
                                                   std::move(opts));
          case 5:
            return std::make_unique<hash_index<5>>(std::move(x),
                                                   std::move(opts));
          case 6:
            return std::make_unique<hash_index<6>>(std::move(x),
                                                   std::move(opts));
          case 7:
            return std::make_unique<hash_index<7>>(std::move(x),
                                                   std::move(opts));
          case 8:
            return std::make_unique<hash_index<8>>(std::move(x),
                                                   std::move(opts));
        }
      }
  }
  return std::make_unique<T>(std::move(x), std::move(opts));
}

template <class T, class Index>
auto add_value_index_factory() {
  return factory<value_index>::add(T{}, make<Index>);
}

template <class T>
auto add_arithmetic_index_factory() {
  static_assert(detail::is_any_v<T, legacy_integer_type, legacy_count_type,
                                 legacy_enumeration_type, legacy_real_type,
                                 legacy_duration_type, legacy_time_type>);
  using concrete_data = type_to_data<T>;
  if constexpr (detail::is_any_v<concrete_data, integer>)
    return add_value_index_factory<
      T, arithmetic_index<typename concrete_data::value_type>>();
  else
    return add_value_index_factory<T, arithmetic_index<concrete_data>>();
}

} // namespace

void factory_traits<value_index>::initialize() {
  add_value_index_factory<legacy_bool_type, arithmetic_index<bool>>();
  add_arithmetic_index_factory<legacy_integer_type>();
  add_arithmetic_index_factory<legacy_count_type>();
  add_arithmetic_index_factory<legacy_real_type>();
  add_arithmetic_index_factory<legacy_duration_type>();
  add_arithmetic_index_factory<legacy_time_type>();
  add_value_index_factory<legacy_enumeration_type, enumeration_index>();
  add_value_index_factory<legacy_address_type, address_index>();
  add_value_index_factory<legacy_subnet_type, subnet_index>();
  add_value_index_factory<legacy_string_type, string_index>();
  add_value_index_factory<legacy_list_type, list_index>();
}

factory_traits<value_index>::key_type
factory_traits<value_index>::key(const legacy_type& t) {
  auto f = [&](const auto& x) {
    using concrete_type = std::decay_t<decltype(x)>;
    if constexpr (std::is_same_v<concrete_type, legacy_alias_type>) {
      return key(x.value_type);
    } else {
      static legacy_type instance = concrete_type{};
      return instance;
    }
  };
  return caf::visit(f, t);
}

} // namespace vast

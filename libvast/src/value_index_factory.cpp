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

#include "vast/value_index_factory.hpp"

#include "vast/base.hpp"
#include "vast/concept/parseable/numeric/integral.hpp"
#include "vast/concept/parseable/vast/base.hpp"
#include "vast/detail/bit.hpp"
#include "vast/detail/type_traits.hpp"
#include "vast/hash_index.hpp"
#include "vast/logger.hpp"
#include "vast/type.hpp"
#include "vast/value_index.hpp"

#include <caf/optional.hpp>
#include <caf/settings.hpp>

#include <cmath>

using namespace std::string_view_literals;

namespace vast {
namespace {

template <class T>
value_index_ptr make(type x, caf::settings opts) {
  using int_type = caf::config_value::integer;
  // The cardinality must be an integer.
  if (auto i = opts.find("cardinality"); i != opts.end()) {
    if (!caf::holds_alternative<int_type>(i->second)) {
      VAST_ERROR_ANON(__func__, "invalid cardinality type");
      return nullptr;
    }
  }
  // The base specification has its own grammar.
  if (auto i = opts.find("base"); i != opts.end()) {
    auto str = caf::get_if<caf::config_value::string>(&i->second);
    if (!str) {
      VAST_ERROR_ANON(__func__, "invalid base type (string type needed)");
      return nullptr;
    }
    if (!parsers::base(*str)) {
      VAST_ERROR_ANON(__func__, "invalid base specification");
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
          VAST_WARNING_ANON(__func__, "got an explicit cardinality of 2^64"
                                      ", using max digest size of 8 bytes");
          return std::make_unique<hash_index<8>>(std::move(x));
        }
        if (!detail::ispow2(*cardinality))
          VAST_WARNING_ANON(__func__, "cardinality not a power of 2");
        // For 2^n unique values, we expect collisions after sqrt(2^n).
        // Thus, we use 2n bits as digest size.
        size_t digest_bits = detail::ispow2(*cardinality)
                               ? (detail::log2p1(*cardinality) - 1) * 2
                               : detail::log2p1(*cardinality) * 2;
        auto digest_bytes = digest_bits / 8;
        if (digest_bits % 8 > 0)
          ++digest_bytes;
        VAST_DEBUG_ANON(__func__, "creating hash index with a digest of",
                        digest_bytes, "bytes");
        if (digest_bytes > 8) {
          VAST_WARNING_ANON(__func__,
                            "expected cardinality exceeds "
                            "maximum digest size, capping at 8 bytes");
          digest_bytes = 8;
        }
        switch (digest_bytes) {
          default:
            VAST_ERROR_ANON(__func__, "invalid digest size", *cardinality);
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
  static_assert(detail::is_any_v<T, integer_type, count_type, enumeration_type,
                                 real_type, duration_type, time_type>);
  using concrete_data = type_to_data<T>;
  return add_value_index_factory<T, arithmetic_index<concrete_data>>();
}

} // namespace <anonymous>

void factory_traits<value_index>::initialize() {
  add_value_index_factory<bool_type, arithmetic_index<bool>>();
  add_arithmetic_index_factory<integer_type>();
  add_arithmetic_index_factory<count_type>();
  add_arithmetic_index_factory<real_type>();
  add_arithmetic_index_factory<duration_type>();
  add_arithmetic_index_factory<time_type>();
  add_value_index_factory<enumeration_type, enumeration_index>();
  add_value_index_factory<address_type, address_index>();
  add_value_index_factory<subnet_type, subnet_index>();
  add_value_index_factory<port_type, port_index>();
  add_value_index_factory<string_type, string_index>();
  add_value_index_factory<list_type, list_index>();
}

factory_traits<value_index>::key_type
factory_traits<value_index>::key(const type& t) {
  auto f = [&](const auto& x) {
    using concrete_type = std::decay_t<decltype(x)>;
    if constexpr (std::is_same_v<concrete_type, alias_type>) {
      return key(x.value_type);
    } else {
      static type instance = concrete_type{};
      return instance;
    }
  };
  return caf::visit(f, t);
}

} // namespace vast

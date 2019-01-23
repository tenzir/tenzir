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

#include <caf/optional.hpp>

#include "vast/base.hpp"
#include "vast/concept/parseable/numeric/integral.hpp"
#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/base.hpp"
#include "vast/detail/type_traits.hpp"
#include "vast/type.hpp"
#include "vast/value_index.hpp"

namespace vast {
namespace {

template <class T>
caf::optional<std::string_view> extract_attribute(const T& x,
                                                  std::string_view key) {
  for (auto& attr : x.attributes())
    if (attr.key == key && attr.value)
      return std::string_view{*attr.value};
  return {};
}

template <class T>
size_t extract_max_size(const T& x, size_t default_value = 1024) {
  if (auto a = extract_attribute(x, "max_size"))
    if (auto max_size = to<size_t>(*a))
      return *max_size;
  return default_value;
}

template <class T>
optional<base> parse_base(const T& x) {
  if (auto a = extract_attribute(x, "base")) {
    if (auto b = to<base>(*a))
      return *b;
    return {};
  }
  // Use base 8 by default, as it yields the best performance on average for
  // VAST's indexing.
  return base::uniform<64>(8);
}

template <class T>
value_index_ptr make(type x) {
  return std::make_unique<T>(std::move(x));
}

template <class T>
value_index_ptr make_arithmetic(type x) {
  static_assert(detail::is_any_v<T, integer_type, count_type, real_type,
                timespan_type, timestamp_type>);
  using concrete_data = type_to_data<T>;
  using value_index_type = arithmetic_index<concrete_data>;
  if (auto base = parse_base(x))
    return std::make_unique<value_index_type>(std::move(x), std::move(*base));
  return nullptr;
}

template <class T, class Index>
auto add_value_index_factory() {
  return factory<value_index>::add(T{}, make<Index>);
}

template <class T>
auto add_arithmetic_index_factory() {
  return factory<value_index>::add(T{}, make_arithmetic<T>);
}

template <class T, class Index>
auto add_container_index_factory() {
  static auto f = [](type x) -> value_index_ptr {
    auto max_size = extract_max_size(x);
    return std::make_unique<Index>(std::move(x), max_size);
  };
  return factory<value_index>::add(T{}, f);
}

} // namespace <anonymous>

void factory_traits<value_index>::initialize() {
  add_value_index_factory<boolean_type, arithmetic_index<boolean>>();
  add_arithmetic_index_factory<integer_type>();
  add_arithmetic_index_factory<count_type>();
  add_arithmetic_index_factory<real_type>();
  add_arithmetic_index_factory<timespan_type>();
  add_arithmetic_index_factory<timestamp_type>();
  add_value_index_factory<address_type, address_index>();
  add_value_index_factory<subnet_type, subnet_index>();
  add_value_index_factory<port_type, port_index>();
  add_container_index_factory<string_type, string_index>();
  add_container_index_factory<vector_type, sequence_index>();
  add_container_index_factory<set_type, sequence_index>();
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

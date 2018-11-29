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

#include <string>
#include <vector>

#include <caf/detail/type_list.hpp>
#include <caf/dictionary.hpp>
#include <caf/fwd.hpp>
#include <caf/none.hpp>
#include <caf/variant.hpp>

#include "vast/concept/printable/to.hpp"

#include "vast/detail/operators.hpp"
#include "vast/detail/steady_map.hpp"
#include "vast/detail/type_traits.hpp"

namespace vast {

/// A JSON data type.
class json : detail::totally_ordered<json> {
public:
  /// A JSON null type.
  using null = caf::none_t;

  /// A JSON bool.
  using boolean = bool;

  /// A JSON number value.
  using number = long double;

  /// A JSON string.
  using string = std::string;

  /// A JSON array.
  using array = std::vector<json>;

  /// A JSON object.
  using object = detail::steady_map<std::string, json>;

  /// The sum type of all possible JSON types.
  using types = caf::detail::type_list<
    null,
    boolean,
    number,
    string,
    array,
    object
  >;

  /// The sum type of all possible JSON types.
  using variant = caf::detail::tl_apply_t<types, caf::variant>;

  /// Default-constructs a null JSON value.
  json() = default;

  json(json&&) = default;

  json(const json&) = default;

  /// Constructs a JSON value.
  /// @tparam The JSON type.
  /// @param x A JSON type.
  template <class T>
  explicit json(T x);

  json& operator=(json&&) = default;

  json& operator=(const json&) = default;

  template <class T>
  json& operator=(T x);

  // -- concepts --------------------------------------------------------------

  variant& get_data() {
    return data_;
  }

  const variant& get_data() const {
    return data_;
  }

  friend bool operator==(const json& x, const json& y) {
    return x.data_ == y.data_;
  }

  friend bool operator<(const json& x, const json& y) {
    return x.data_ < y.data_;
  }

  template <class... Ts>
  static json::array make_array(Ts&&... xs) {
    return json::array{json{std::forward<Ts>(xs)}...};
  }

private:
  variant data_;
};

namespace detail {

template <
  class T,
  int = std::is_convertible_v<T, json::number>
        && !std::is_same_v<T, json::boolean>
        ? 1
        : (std::is_convertible_v<T, std::string> ? 2 : 0)
>
struct to_json_helper;

template <class T>
struct to_json_helper<T, 0> {
  using type = T;
};

template <class T>
struct to_json_helper<T, 1> {
  using type = json::number;
};

template <class T>
struct to_json_helper<T, 2> {
  using type = json::string;
};

} // namespace detail

/// Converts an arbitrary type to the corresponding JSON type.
/// @relates json
template <class T>
using to_json_type = typename detail::to_json_helper<std::decay_t<T>>::type;

template <class T>
json::json(T x) : data_(to_json_type<T>(std::move(x))) {
  // nop
}

template <class T>
json& json::operator=(T x) {
  data_ = to_json_type<T>(std::move(x));
  return *this;
}

/// @relates json
inline bool convert(bool b, json& j) {
  j = json::boolean{b};
  return true;
}

/// @relates json
template <class T>
bool convert(T x, json& j) {
  if constexpr (std::is_arithmetic_v<T>)
    j = json::number(x);
  else if constexpr (std::is_convertible_v<T, std::string>)
    j = std::string(std::forward<T>(x));
  else
    static_assert(detail::always_false_v<T>);
  return true;
}

/// @relates json
template <class T>
bool convert(const std::vector<T>& v, json& j) {
  json::array a;
  for (auto& x : v) {
    json i;
    if (!convert(x, i))
      return false;
    a.push_back(std::move(i));
  };
  j = std::move(a);
  return true;
}

/// @relates json
template <class K, class V>
bool convert(const std::map<K, V>& m, json& j) {
  json::object o;
  for (auto& p : m) {
    auto k = to<std::string>(p.first);
    json v;
    if (!(k && convert(p.second, v)))
      return false;
    o.emplace(std::move(*k), std::move(v));
  };
  j = std::move(o);
  return true;
}

/// @relates json
template <class V>
bool convert(const caf::dictionary<V>& xs, json& j) {
  return convert(xs.container(), j);
}

/// @relates json
bool convert(const caf::config_value& x, json& j);

/// @relates json
template <class T, class... Opts>
json to_json(const T& x, Opts&&... opts) {
  json j;
  if (convert(x, j, std::forward<Opts>(opts)...))
    return j;
  return {};
}

} // namespace vast

namespace caf {

template <>
struct sum_type_access<vast::json> : default_sum_type_access<vast::json> {};

} // namespace caf

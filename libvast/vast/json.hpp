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

#include "vast/detail/narrow.hpp"
#include "vast/detail/operators.hpp"
#include "vast/detail/overload.hpp"
#include "vast/detail/stable_map.hpp"
#include "vast/detail/stack_vector.hpp"
#include "vast/detail/type_traits.hpp"

#include <caf/config_value.hpp>
#include <caf/detail/type_list.hpp>
#include <caf/dictionary.hpp>
#include <caf/fwd.hpp>
#include <caf/none.hpp>
#include <caf/variant.hpp>

#include <string>
#include <vector>

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
  using object = detail::stable_map<string, json>;

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

  template <typename T,
            typename = std::enable_if_t<std::is_constructible_v<json, T>>>
  friend bool operator==(const json& x, const T& y) {
    return operator==(x, json{y});
  }

  template <typename T,
            typename = std::enable_if_t<std::is_constructible_v<json, T>>>
  friend bool operator==(const T& x, const json& y) {
    return operator==(json{x}, y);
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
    j = detail::narrow_cast<json::number>(x);
  else if constexpr (std::is_convertible_v<T, json::string>)
    j = json::string{std::move(x)};
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
template <class V>
bool convert(const std::map<std::string, V>& m, json& j) {
  json::object o;
  for (auto& p : m) {
    json v;
    if (!convert(p.second, v))
      return false;
    o.emplace(p.first, std::move(v));
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

json::object combine(const json::object& lhs, const json::object& rhs);

namespace detail {

template <class F>
void each_field_impl(const json& x, F f,
                     detail::stack_vector<std::string_view, 64>& prefix) {
  static_assert(
    std::is_invocable_v<F, detail::stack_vector<std::string_view, 64>&,
                        const json&>,
    "f does not match the required signature");
  caf::visit(
    // This comment exists merely for clang-format.
    detail::overload(
      // For json objects we recurse deeper.
      [&](const json::object& obj) {
        for (const auto& [k, v] : obj) {
          prefix.emplace_back(k);
          each_field_impl(v, f, prefix);
          prefix.pop_back();
        }
      },
      // For everything else, we have reached a leaf and invoke the functor.
      [&](const auto& j) { std::invoke(std::move(f), prefix, json{j}); }),
    x);
}

} // namespace detail

/// Invoke a functor for each leaf field of the JSON object, carrying along a
/// list of prefixes that reflect the parent objects keys.
/// @relates json
template <class F>
void each_field(const json& x, F f) {
  auto prefix = detail::stack_vector<std::string_view, 64>{};
  detail::each_field_impl(x, std::move(f), prefix);
}

/// Invoke a functor for each leaf field of the JSON object, carrying along a
/// list of prefixes that reflect the parent objects keys.
/// @relates json::object
template <class F>
void each_field(const json::object& x, F f) {
  auto prefix = detail::stack_vector<std::string_view, 64>{};
  for (const auto& [k, v] : x) {
    prefix.emplace_back(k);
    detail::each_field_impl(x, std::move(f), prefix);
    prefix.pop_back();
  }
}

} // namespace vast

namespace caf {

template <>
struct sum_type_access<vast::json> : default_sum_type_access<vast::json> {};

} // namespace caf

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

#include "vast/concept/printable/to.hpp"
#include "vast/detail/operators.hpp"
#include "vast/detail/steady_map.hpp"
#include "vast/none.hpp"
#include "vast/variant.hpp"

namespace vast {

/// A JSON data type.
class json : detail::totally_ordered<json> {
public:
  /// A JSON null type.
  using null = none;

  /// A JSON bool.
  using boolean = bool;

  /// A JSON number value.
  using number = long double;

  /// A JSON string.
  using string = std::string;

  /// A JSON array.
  struct array;

  /// A JSON object.
  struct object;

  /// Meta-function that converts a type into a JSON value.
  /// If conversion is impossible it returns `std::false_type`.
  template <class T>
  using json_value = std::conditional_t<
    std::is_same<T, none>::value,
    null,
    std::conditional_t<
      std::is_same<T, bool>::value,
      boolean,
      std::conditional_t<
        std::is_convertible<T, number>::value,
        number,
        std::conditional_t<
          std::is_convertible<T, std::string>::value,
          string,
          std::conditional_t<
            std::is_same<T, array>::value,
            array,
            std::conditional_t<
              std::is_same<T, object>::value,
              object,
              std::false_type
            >
          >
        >
      >
    >
  >;

  /// Maps an arbitrary type to a json type.
  template <class T>
  using jsonize = json_value<std::decay_t<T>>;

  /// A sequence of JSON values.
  struct array : std::vector<json> {
    using super = std::vector<json>;
    using super::vector;

    array() = default;

    array(const super& s) : super(s) {
    }

    array(super&& s) : super(std::move(s)) {
    }
  };

  /// An associative data structure exposing key-value pairs with unique keys.
  struct object : detail::steady_map<string, json> {
    using super = detail::steady_map<string, json>;
    using super::vector_map;

    object() = default;

    object(const super& s) : super(s) {
    }

    object(super&& s) : super(std::move(s)) {
    }
  };

  /// Default-constructs a null JSON value.
  json() = default;

  /// Constructs a JSON value.
  /// @tparam The JSON type.
  /// @param x A JSON type.
  template <
    class T,
    class =
      std::enable_if_t<!std::is_same<std::false_type, jsonize<T>>::value>
  >
  json(T&& x)
    : value_(jsonize<T>(std::forward<T>(x))) {
  }

  friend auto& expose(json& j) {
    return j.value_;
  }

private:
  using variant_type = variant<
    null,
    boolean,
    number,
    string,
    array,
    object
  >;

  variant_type value_;

private:
  struct less_than {
    template <class T>
    bool operator()(const T& x, const T& y) {
      return x < y;
    }

    template <class T, class U>
    bool operator()(const T&, const U&) {
      return false;
    }
  };

  struct equals {
    template <class T>
    bool operator()(const T& x, const T& y) {
      return x == y;
    }

    template <class T, class U>
    bool operator()(const T&, const U&) {
      return false;
    }
  };

  friend bool operator<(const json& x, const json& y) {
    if (x.value_.index() == y.value_.index())
      return visit(less_than{}, x.value_, y.value_);
    else
      return x.value_.index() < y.value_.index();
  }

  friend bool operator==(const json& x, const json& y) {
    return visit(equals{}, x.value_, y.value_);
  }
};

inline bool convert(bool b, json& j) {
  j = b;
  return true;
}

template <class T>
bool convert(T x, json& j) {
  if constexpr (std::is_arithmetic<T>::value) {
    j = json::number(x);
    return true;
  } else if constexpr (std::is_convertible_v<T, std::string>) {
    j = std::string{std::forward<T>(x)};
    return true;
  } else {
    static_assert(!std::is_same_v<T, T>,
                  "T is neither arithmetic nor convertible to std::string");
  }
}

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

template <class T, class... Opts>
json to_json(const T& x, Opts&&... opts) {
  json j;
  if (convert(x, j, std::forward<Opts>(opts)...))
    return j;
  return {};
}

} // namespace vast


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

#ifndef VAST_VALUE_HPP
#define VAST_VALUE_HPP

#include <type_traits>

#include "vast/data.hpp"
#include "vast/type.hpp"
#include "vast/detail/type_traits.hpp"

namespace vast {

/// Typed representation of data.
class value {
  friend access;

public:
  /// Constructs a type-safe value by checking whether the given data matches
  /// the given type.
  /// @param d The data for the value.
  /// @param t The type *d* shall have.
  /// @returns If `t.check(d)` then a value containing *d* and `nil` otherwise.
  static value make(vast::data d, vast::type t) {
    return type_check(t, d) ? value{std::move(d), std::move(t)} : nil;
  }

  /// Constructs an invalid value.
  /// Same as default-construction, but also enables statements like `v = nil`.
  value(none = nil) {
  }

  /// Constructs an untyped value from data.
  /// @param x The data for the value.
  template <
    class T,
    class = detail::disable_if_t<
      detail::is_same_or_derived<value, T>::value
      || std::is_same<data_type<T>, std::false_type>::value
    >
  >
  value(T&& x)
    : data_{std::forward<T>(x)} {
  }

  /// Constructs a typed value from data.
  /// @param d The data for the value.
  /// @param t The type of *d*.
  value(vast::data d, vast::type t) : data_{std::move(d)}, type_{std::move(t)} {
  }

  /// Constructs a typed value from anything convertible to data.
  /// @tparam T A type convertible to data.
  /// @param x An instance of type `T`.
  /// @param t The type of the value.
  /// @post If `! t.check(d)` then `*this = nil`.
  template <class T>
  value(T&& x, vast::type t)
    : value{vast::data(std::forward<T>(x)), std::move(t)} {
  }

  /// Constructs an untyped value.
  /// @param x The data to construct the value from.
  value(vast::data x) : data_{std::move(x)} {
  }

  /// Sets the type of the value.
  /// @param t The new type of the value.
  /// @returns `true` if the value had no data or if the type check succeeded.
  bool type(const vast::type& t);

  /// Retrieves the type of the value.
  /// @returns The type of the value.
  const vast::type& type() const;

  /// Retrieves the data of the value.
  /// @returns The value data.
  const vast::data& data() const;

  friend bool operator==(const value& lhs, const value& rhs);
  friend bool operator!=(const value& lhs, const value& rhs);
  friend bool operator<(const value& lhs, const value& rhs);
  friend bool operator<=(const value& lhs, const value& rhs);
  friend bool operator>=(const value& lhs, const value& rhs);
  friend bool operator>(const value& lhs, const value& rhs);

  template <class Inspector>
  friend auto inspect(Inspector&f, value& v) {
    return f(v.data_, v.type_);
  }

  friend detail::data_variant& expose(value& v);

private:
  vast::data data_;
  vast::type type_;
};

/// Flattens a value if it is a record.
/// @param v The value to to flatten.
/// @returns The flattened value or *v* if not a record.
value flatten(const value& v);

bool convert(const value& v, json& j);

} // namespace vast

#endif

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

#include <type_traits>

#include <caf/default_sum_type_access.hpp>

#include "vast/data.hpp"
#include "vast/type.hpp"

namespace vast {

/// Typed representation of data.
class value {
  friend access;

public:
  using types = vast::data::types;

  /// Constructs a type-safe value by checking whether the given data matches
  /// the given type.
  /// @param d The data for the value.
  /// @param t The type *d* shall have.
  /// @returns If `t.check(d)` then a value containing *d* and `nil` otherwise.
  static value make(vast::data d, vast::type t) {
    return type_check(t, d) ? value{std::move(d), std::move(t)} : caf::none;
  }

  /// Constructs an invalid value.
  /// Same as default-construction, but also enables statements like `v = nil`.
  value(caf::none_t = caf::none) {
    // nop
  }

  /// Constructs an untyped value from data.
  /// @param x The data for the value.
  template <class T, class = std::enable_if_t<std::is_constructible_v<data, T>>>
  value(T&& x) : data_{std::forward<T>(x)} {
    // nop
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

  /// @cond PRIVATE

  data::variant& get_data() {
    return data_.get_data();
  }

  const data::variant& get_data() const {
    return data_.get_data();
  }

  template <class Inspector>
  friend auto inspect(Inspector&f, value& v) {
    return f(v.data_, v.type_);
  }

  /// @endond

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

namespace caf {

template <>
struct sum_type_access<vast::value> : default_sum_type_access<vast::value> {};

} // namespace caf

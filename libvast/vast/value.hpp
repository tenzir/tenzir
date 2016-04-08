#ifndef VAST_VALUE_HPP
#define VAST_VALUE_HPP

#include <type_traits>

#include "vast/data.hpp"
#include "vast/none.hpp"
#include "vast/type.hpp"
#include "vast/util/meta.hpp"

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
    return t.check(d) ? value{std::move(d), std::move(t)} : nil;
  }

  /// Constructs an invalid value.
  /// Same as default-construction, but also enables statements like `v = nil`.
  value(none = nil) {
  }

  /// Constructs an untyped value.
  /// @param x The data for the value.
  template <
    typename T,
    typename = util::disable_if_t<
      util::is_same_or_derived<value, T>::value
      || std::is_same<vast::data::type<T>, std::false_type>::value
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
  template <typename T>
  value(T&& x, vast::type t)
    : value{vast::data(std::forward<T>(x)), std::move(t)} {
  }

  /// Constructs an untyped value.
  /// @param x The data to construct the value from.
  value(vast::data x) : data_{std::move(x)} {
  }

  friend bool operator==(value const& lhs, value const& rhs);
  friend bool operator!=(value const& lhs, value const& rhs);
  friend bool operator<(value const& lhs, value const& rhs);
  friend bool operator<=(value const& lhs, value const& rhs);
  friend bool operator>=(value const& lhs, value const& rhs);
  friend bool operator>(value const& lhs, value const& rhs);

  /// Sets the type of the value.
  /// @param t The new type of the value.
  /// @returns `true` if the value had no data or if the type check succeeded.
  bool type(vast::type const& t);

  /// Retrieves the type of the value.
  /// @returns The type of the value.
  vast::type const& type() const;

  /// Retrieves the data of the value.
  /// @returns The value data.
  vast::data const& data() const;

  friend vast::data::variant_type& expose(value& v);
  friend vast::data::variant_type const& expose(value const& v);

private:
  vast::data data_;
  vast::type type_;
};

/// Flattens a value if it is a record.
/// @param v The value to to flatten.
/// @returns The flattened value or *v* if not a record.
value flatten(value const& v);

} // namespace vast

#endif

#ifndef VAST_VALUE_H
#define VAST_VALUE_H

#include "vast/data.h"

namespace vast {

/// Typed representation of data.
class value
{
public:
  /// Constructs an invalid value.
  /// Same as default-construction, but also enables statements like `v = nil`.
  value(none = nil) {}

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
    : data_{x}
  {
  }

  /// Constructs a typed value from data.
  /// @param d The data for the value.
  /// @param t The type of *d*.
  /// @post If `! t.check(d)` then `*this = nil`.
  value(vast::data d, vast::type t)
    : data_{std::move(d)},
      type_{std::move(t)}
  {
    if (! type_.check(data_))
    {
      data_ = nil;
      type_ = {};
    }
  }

  /// Constructs a typed value from anything convertible to data.
  /// @tparam T A type convertible to data.
  /// @param x An instance of type `T`.
  /// @param t The type of the value.
  /// @post If `! t.check(d)` then `*this = nil`.
  template <typename T>
  value(T&& x, vast::type t)
    : value{vast::data(std::forward<T>(x)), std::move(t)}
  {
  }

  /// Constructs an untyped value.
  /// @param x The data to construct the value from.
  value(vast::data x) : data_{std::move(x)} {}

  /// Sets the type of the value.
  /// @param t The new type of the value.
  /// @returns `true` if the value had no data or if the type check succeeded.
  bool type(vast::type const& t);

  /// Retrieves the type of the value.
  /// @returns The type of the value.
  vast::type const& type() const;

  /// Retrieves the data of the value.
  /// @returns The value data.
  vast::data const& data() const
  {
    return data_;
  }

private:
  friend access;
  void serialize(serializer& sink) const;
  void deserialize(deserializer& source);

  friend vast::data::variant_type& expose(value& v);
  friend vast::data::variant_type const& expose(value const& v);

  friend bool operator==(value const& lhs, value const& rhs);
  friend bool operator!=(value const& lhs, value const& rhs);
  friend bool operator<(value const& lhs, value const& rhs);
  friend bool operator<=(value const& lhs, value const& rhs);
  friend bool operator>=(value const& lhs, value const& rhs);
  friend bool operator>(value const& lhs, value const& rhs);

  vast::data data_;
  vast::type type_;
};

template <typename Iterator>
trial<void> print(value const& v, Iterator&& out)
{
  return print(v.data(), out);
}

template <typename Iterator>
trial<void> parse(value& v, Iterator& begin, Iterator end,
                  type t = {},
                  std::string const& set_sep = ", ",
                  std::string const& set_left = "{",
                  std::string const& set_right = "}",
                  std::string const& vec_sep = ", ",
                  std::string const& vec_left = "[",
                  std::string const& vec_right = "]",
                  std::string const& rec_sep = ", ",
                  std::string const& rec_left = "(",
                  std::string const& rec_right = ")",
                  std::string const& esc = "\\")
{
  data d;
  auto p = parse(d, begin, end, t,
                 set_sep, set_left, set_right,
                 vec_sep, vec_left, vec_right,
                 rec_sep, rec_left, rec_right,
                 esc);
  if (! p)
    return p;

  v = {std::move(d), std::move(t)};

  return nothing;
}

trial<void> convert(value const& v, util::json& j);

} // namespace vast

#endif

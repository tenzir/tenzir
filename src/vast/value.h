#ifndef VAST_VALUE_H
#define VAST_VALUE_H

#include "vast/address.h"
#include "vast/port.h"
#include "vast/prefix.h"
#include "vast/regex.h"
#include "vast/string.h"
#include "vast/time.h"
#include "vast/value_type.h"
#include "vast/util/parse.h"
#include "vast/util/print.h"

namespace vast {

/// The invalid or optional value.
struct invalid_value { };
constexpr invalid_value invalid = {};

template <typename Iterator>
bool print(Iterator& out, invalid_value)
{
  static constexpr auto str = "<invalid>";
  out = std::copy(str, str + 9, out);
  return true;
}

namespace detail {
template <typename, typename>
struct visit_impl;
} // namespace detail

/// A discriminated union container for numerous types with value semantics.
/// A value has one of three states:
///
///     1. invalid
///     2. empty but typed
///     3. engaged with a value.
///
/// An *invalid* value is untyped and has not been set. An empty or *nil* value
/// has a type, but has not yet been set. This state exists to model
/// `optional<T>` semantics where `T` is a value type. A nil value is thus
/// equivalent to a disengaged optional value. Finally, an *engaged* type
/// contains a valid instance of some value type `T`.
class value : util::parsable<value>,
              util::printable<value>
{
  template <typename, typename>
  friend struct detail::visit_impl;

public:
  /// Visits a value (single dispatch).
  /// @param v The value to visit.
  /// @param f The visitor to apply to *v*.
  template <typename F>
  typename F::result_type
  static visit(value const& v, F f);

  template <typename F>
  typename F::result_type
  static visit(value&, F);

  /// Visits a value (double dispatch).
  /// @param v1 The first value.
  /// @param v2 The second value.
  /// @param f The visitor to apply to *v1* and *v2*.
  template <typename F>
  typename F::result_type
  static visit(value const& v1, value const& v2, F f);

  template <typename F>
  typename F::result_type
  static visit(value&, value const&, F);

  template <typename F>
  typename F::result_type
  static visit(value const&, value&, F);

  template <typename F>
  typename F::result_type
  static visit(value&, value&, F);

  /// Parses an arbitrary value.
  ///
  /// @param str The string to parse as value.
  ///
  /// @returns `invalid` if *str* does not describe a value and the parsed
  /// valued otherwise.
  static value parse(std::string const& str);

  /// Default-constructs an invalid value.
  value(invalid_value = vast::invalid);

  /// Constructs a disengaged value with a given type.
  /// @param t The type of the value.
  explicit value(value_type t);

  value(bool b);
  value(int i);
  value(unsigned int i);
  value(long l);
  value(unsigned long l);
  value(long long ll);
  value(unsigned long long ll);
  value(double d);
  value(time_range r);
  value(time_point t);
  value(char c);
  value(char const* s);
  value(char const* s, size_t n);
  value(std::string const& s);
  value(string s);
  value(regex r);
  value(address a);
  value(prefix p);
  value(port p);
  value(record r);
  value(table t);
  value(std::initializer_list<value> list);
  value(std::initializer_list<std::pair<value const, value>> list);

  template <typename Rep, typename Period>
  value(std::chrono::duration<Rep, Period> duration)
  {
    new (&data_.time_range_) time_range{duration};
    data_.type(time_range_type);
    data_.engage();
  }

  template <typename Clock, typename Duration>
  value(std::chrono::time_point<Clock, Duration> time)
  {
    new (&data_.time_point_) time_point{time};
    data_.type(time_point_type);
    data_.engage();
  }

  ~value() = default;
  value(value const& other) = default;
  value(value&& other) = default;
  value& operator=(value const&) = default;
  value& operator=(value&&) = default;

  /// Checks whether the value is engaged.
  /// @returns `true` iff the value is engaged.
  /// @note An invalid value is always disengaged.
  explicit operator bool() const;

  /// Checks whether the value is *nil*.
  /// @returns `true` if the value has a type but has not yet been set.
  /// @note An invalid value is not *nil*.
  bool nil() const;

  /// Checks whether the value is the invalid value.
  /// @returns `true` if `*this == invalid`.
  /// @note An invalid value is not *nil*.
  bool invalid() const;

  /// Returns the type information of the value.
  /// @returns The type of the value.
  value_type which() const;

  /// Accesses the currently stored data in a type safe manner. The caller
  /// shall ensure that that value is engaged beforehand.
  ///
  /// @throws `std::bad_cast` if the contained data is not of type `T` or
  /// if the value is not engaged.
  template <typename T>
  T& get();

  template <typename T>
  T const& get() const;

  /// Disengages the value while keeping the type information. After calling
  /// this function, the value has relinquished its internal resources.
  void clear();

private:
  union data
  {
    data();
    data(data const& other);
    data(data&& other);
    ~data();
    data& operator=(data other);

    void construct(value_type t);

    value_type type() const;
    void type(value_type t);
    void engage();
    bool engaged() const;
    uint8_t tag() const;

    void serialize(serializer& sink) const;
    void deserialize(deserializer& source);

    bool bool_;
    int64_t int_;
    uint64_t uint_;
    double double_;
    time_range time_range_;
    time_point time_point_;
    string string_;
    std::unique_ptr<regex> regex_;
    address address_;
    prefix prefix_;
    port port_;
    std::unique_ptr<record> record_;
    std::unique_ptr<table> table_;
  };

  data data_;

private:
  friend access;

  void serialize(serializer& sink) const;
  void deserialize(deserializer& source);

  template <typename Iterator>
  bool parse(Iterator& start, Iterator end, value_type type)
  {
#define VAST_PARSE_CASE(type)                      \
        {                                          \
          type t;                                  \
          if (! extract(start, end, t))            \
            return false;                          \
          *this = t;                               \
          return true;                             \
        }
    switch (type)
    {
      default:
        throw std::logic_error("unknown value parser");
      case bool_type:
        VAST_PARSE_CASE(bool)
      case int_type:
        VAST_PARSE_CASE(int64_t)
      case uint_type:
        VAST_PARSE_CASE(uint64_t)
      case double_type:
        VAST_PARSE_CASE(double)
      case time_range_type:
        VAST_PARSE_CASE(time_range)
      case time_point_type:
        VAST_PARSE_CASE(time_point)
      case string_type:
        VAST_PARSE_CASE(string)
      case regex_type:
        VAST_PARSE_CASE(regex)
      case address_type:
        VAST_PARSE_CASE(address)
      case prefix_type:
        VAST_PARSE_CASE(prefix)
      case port_type:
        VAST_PARSE_CASE(port)
    }
#undef VAST_PARSE_CASE
  }

  template <typename Iterator>
  struct value_printer
  {
    typedef bool result_type;

    value_printer(Iterator& out)
      : out(out)
    {
    }

    bool operator()(value_type t) const
    {
      *out++ = '<';
      if (! render(out, t))
        return false;
      *out++ = '>';
      return true;
    }

    template <typename T>
    bool operator()(T const& x) const
    {
      return render(out, x);
    }

    bool operator()(string const& str) const
    {
      *out++ = '"';
      if (! render(out, str.escape()))
        return false;
      *out++ = '"';
      return true;
    }

    Iterator& out;
  };

  template <typename Iterator>
  bool print(Iterator& out) const
  {
    return value::visit(*this, value_printer<Iterator>(out));
  }
};

// Relational operators
bool operator==(value const& x, value const& y);
bool operator<(value const& x, value const& y);
bool operator!=(value const& x, value const& y);
bool operator>(value const& x, value const& y);
bool operator<=(value const& x, value const& y);
bool operator>=(value const& x, value const& y);

// Arithmetic operators
value operator+(value const& x, value const& y);
value operator-(value const& x, value const& y);
value operator*(value const& x, value const& y);
value operator/(value const& x, value const& y);
value operator%(value const& x, value const& y);
value operator-(value const& x);

// Bitwise operators
value operator&(value const& x, value const& y);
value operator|(value const& x, value const& y);
value operator^(value const& x, value const& y);
value operator<<(value const& x, value const& y);
value operator>>(value const& x, value const& y);
value operator~(value const& x);

namespace detail {

template <typename F, typename T>
struct value_bind_impl
{
  typedef typename F::result_type result_type;

  value_bind_impl(F f, T& x)
    : x(x), f(f)
  {
  }

  template <typename U>
  typename F::result_type operator()(U& y) const
  {
    return f(x, y);
  }

  template <typename U>
  typename F::result_type operator()(U const& y) const
  {
    return f(x, y);
  }

  T& x;
  F f;
};

template <typename F, typename T>
value_bind_impl<F, T const> value_bind(F f, T const& x)
{
  return value_bind_impl<F, T const>(f, x);
}

template <typename F, typename T>
value_bind_impl<F, T> value_bind(F f, T& x)
{
  return value_bind_impl<F, T>(f, x);
}

template <typename V1, typename V2 = V1>
struct visit_impl
{
  // Single dispatch.
  template <typename F>
  typename F::result_type
  static apply(V1& x, F f)
  {
    if (x.nil())
      return f(x.which());
    switch (x.which())
    {
      default:
        throw std::runtime_error("corrupt value type");
        break;
      case invalid_type:
        return f(invalid);
      case bool_type:
        return f(x.data_.bool_);
      case int_type:
        return f(x.data_.int_);
      case uint_type:
        return f(x.data_.uint_);
      case double_type:
        return f(x.data_.double_);
      case time_range_type:
        return f(x.data_.time_range_);
      case time_point_type:
        return f(x.data_.time_point_);
      case string_type:
        return f(x.data_.string_);
      case regex_type:
        return f(*x.data_.regex_);
      case address_type:
        return f(x.data_.address_);
      case prefix_type:
        return f(x.data_.prefix_);
      case port_type:
        return f(x.data_.port_);
      case record_type:
        return f(*x.data_.record_);
      case table_type:
        return f(*x.data_.table_);
    }
  }

  // Double dispatch.
  template <typename F>
  typename F::result_type
  static apply(V1& x, V2& y, F f)
  {
    if (x.nil())
      return visit_impl::apply(y, value_bind(f, x.which()));
    switch (x.which())
    {
      default:
        throw std::runtime_error("corrupt value type");
        break;
      case invalid_type:
        return visit_impl::apply(y, value_bind(f, invalid));
      case bool_type:
        return visit_impl::apply(y, value_bind(f, x.data_.bool_));
      case int_type:
        return visit_impl::apply(y, value_bind(f, x.data_.int_));
      case uint_type:
        return visit_impl::apply(y, value_bind(f, x.data_.uint_));
      case double_type:
        return visit_impl::apply(y, value_bind(f, x.data_.double_));
      case time_range_type:
        return visit_impl::apply(y, value_bind(f, x.data_.time_range_));
      case time_point_type:
        return visit_impl::apply(y, value_bind(f, x.data_.time_point_));
      case string_type:
        return visit_impl::apply(y, value_bind(f, x.data_.string_));
      case regex_type:
        return visit_impl::apply(y, value_bind(f, *x.data_.regex_));
      case address_type:
        return visit_impl::apply(y, value_bind(f, x.data_.address_));
      case prefix_type:
        return visit_impl::apply(y, value_bind(f, x.data_.prefix_));
      case port_type:
        return visit_impl::apply(y, value_bind(f, x.data_.port_));
      case record_type:
        return visit_impl::apply(y, value_bind(f, *x.data_.record_));
      case table_type:
        return visit_impl::apply(y, value_bind(f, *x.data_.table_));
    }
  }
};

} // namespace detail

template <typename F>
typename F::result_type
inline value::visit(value const& x, F f)
{
  return detail::visit_impl<value const>::apply(x, f);
}

template <typename F>
typename F::result_type
inline value::visit(value& x, F f)
{
  return detail::visit_impl<value>::apply(x, f);
}

template <typename F>
typename F::result_type
inline value::visit(value const& x, value const& y, F f)
{
  return detail::visit_impl<value const, value const>::apply(x, y, f);
}

template <typename F>
typename F::result_type
inline value::visit(value const& x, value& y, F f)
{
  return detail::visit_impl<value const, value>::apply(x, y, f);
}

template <typename F>
typename F::result_type
inline value::visit(value& x, value const& y, F f)
{
  return detail::visit_impl<value, value const>::apply(x, y, f);
}

template <typename F>
typename F::result_type
inline value::visit(value& x, value& y, F f)
{
  return detail::visit_impl<value, value>::apply(x, y, f);
}

namespace detail {

template <typename T>
struct getter
{
  typedef T* result_type;

  result_type operator()(T& val) const
  {
    return &val;
  }

  template <typename U>
  result_type operator()(U const&) const
  {
    throw std::bad_cast();
  }
};

} // namespace detail

template <typename T>
inline T& value::get()
{
  return *value::visit(*this, detail::getter<T>());
}

template <typename T>
inline T const& value::get() const
{
  return *value::visit(*this, detail::getter<T const>());
}

} // namespace vast

#endif

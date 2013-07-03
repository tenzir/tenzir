#ifndef VAST_VALUE_H
#define VAST_VALUE_H

#include "vast/address.h"
#include "vast/exception.h"
#include "vast/port.h"
#include "vast/prefix.h"
#include "vast/regex.h"
#include "vast/string.h"
#include "vast/time.h"
#include "value_type.h"

namespace vast {

namespace detail {
template <typename, typename>
struct visit_impl;
} // namespace detail

/// The invalid or optional value.
struct invalid_value {};
invalid_value const invalid = {};

/// The nil (empty) value.
struct nil_value {};
nil_value const nil = {};

/// A value.
class value
{
public:
  /// Default-constructs a value.
  value(invalid_value = invalid);

  /// Constructs a nil value.
  value(nil_value);

  value(value_type t);
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
  value(vector v);
  value(set s);
  value(table t);
  value(record r);
  value(value_type type, std::initializer_list<value> list);
  value(address a);
  value(prefix p);
  value(port p);

  template <typename Rep, typename Period>
  value(std::chrono::duration<Rep, Period> duration)
  {
    new (&data_.range) time_range(duration);
    type(time_range_type);
  }

  template <typename Clock, typename Duration>
  value(std::chrono::time_point<Clock, Duration> time)
  {
    new (&data_.time) time_point(time);
    type(time_point_type);
  }

  /// Copy-constructs a value.
  /// @param other The value to copy.
  value(value const& other);

  /// Move-constructs a value.
  /// @param other The value to move.
  value(value&& other);

  /// Destroys a value.
  ~value() = default;

  /// Assigns another value to this instance.
  /// @param other The RHS of the assignment.
  value& operator=(value other);

  /// Releases all internal resources and resets the value to the invalid type.
  void clear();

  /// Returns the type information of the value.
  /// @return The type of the value.
  value_type which() const;

  /// Accesses the currently stored data in a type safe manner.
  /// Throws an @c std::bad_cast if the currently stored data is not of type
  /// @a T.
  template <typename T>
  T& get();

  template <typename T>
  T const& get() const;

  template <typename F>
  typename F::result_type
  static visit(value const&, F);

  template <typename F>
  typename F::result_type
  static visit(value&, F);

  template <typename F>
  typename F::result_type
  static visit(value const&, value const&, F);

  template <typename F>
  typename F::result_type
  static visit(value&, value const&, F);

  template <typename F>
  typename F::result_type
  static visit(value const&, value&, F);

  template <typename F>
  typename F::result_type
  static visit(value&, value&, F);

private:
  template <class V1, class V2>
  friend struct detail::visit_impl;

  //
  // Serialization
  //

  friend access;
  void serialize(serializer& sink) const;
  void deserialize(deserializer& source);

  //
  // Internal state management
  //
  
  /// Retrieves the type information of the value.
  /// @return The type of this value.
  value_type type() const;

  /// Sets the type information of the value.
  /// @param i The type of this value.
  void type(value_type i);

  union data
  {
    data();
    data(data const& other);
    data(data&& other);
    ~data();
    data& operator=(data other);

    value_type type() const;
    void type(value_type t);

    bool boolean;
    int64_t integer;
    uint64_t uinteger;
    double dbl;
    time_range range;
    time_point time;
    string str;
    std::unique_ptr<regex> rx;
    std::unique_ptr<vector> vec;
    std::unique_ptr<set> st;
    std::unique_ptr<table> tbl;
    std::unique_ptr<record> rec;
    address addr;
    prefix pfx;
    port prt;
  };

  data data_;
};

// Relational operators
bool operator==(value const& x, value const& y);
bool operator<(value const& x, value const& y);
bool operator!=(value const& x, value const& y);
bool operator>(value const& x, value const& y);
bool operator<=(value const& x, value const& y);
bool operator>=(value const& x, value const& y);

// Logical operators
bool operator&&(value const& x, value const& y);
bool operator||(value const& x, value const& y);
bool operator!(value const& x);

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

std::string to_string(value const& v);
std::string to_string(invalid_value);
std::string to_string(nil_value);
std::ostream& operator<<(std::ostream& out, value const& v);

namespace detail {

template <typename F, typename T>
struct value_bind_impl
{
  typedef typename F::result_type result_type;

  value_bind_impl(F f, T& x)
    : x(x)
      , f(f)
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
  /// Single dispatch.
  template <typename F>
  typename F::result_type
  static apply(V1& x, F f)
  {
    switch (x.type())
    {
      default:
        throw error::bad_type("corrupt value type", x.type());
        break;
      case invalid_type:
        return f(invalid);
      case nil_type:
        return f(nil);
      case bool_type:
        return f(x.data_.boolean);
      case int_type:
        return f(x.data_.integer);
      case uint_type:
        return f(x.data_.uinteger);
      case double_type:
        return f(x.data_.dbl);
      case time_range_type:
        return f(x.data_.range);
      case time_point_type:
        return f(x.data_.time);
      case string_type:
        return f(x.data_.str);
      case regex_type:
        return f(*x.data_.rx);
      case vector_type:
        return f(*x.data_.vec);
      case set_type:
        return f(*x.data_.st);
      case table_type:
        return f(*x.data_.tbl);
      case record_type:
        return f(*x.data_.rec);
      case address_type:
        return f(x.data_.addr);
      case prefix_type:
        return f(x.data_.pfx);
      case port_type:
        return f(x.data_.prt);
    }
  }

  /// Double dispatch.
  template <typename F>
  typename F::result_type
  static apply(V1& x, V2& y, F f)
  {
    switch (x.type())
    {
      default:
        throw error::bad_type("corrupt value type", x.type());
        break;
      case invalid_type:
        return visit_impl::apply(y, value_bind(f, invalid));
      case nil_type:
        return visit_impl::apply(y, value_bind(f, nil));
      case bool_type:
        return visit_impl::apply(y, value_bind(f, x.data_.boolean));
      case int_type:
        return visit_impl::apply(y, value_bind(f, x.data_.integer));
      case uint_type:
        return visit_impl::apply(y, value_bind(f, x.data_.uinteger));
      case double_type:
        return visit_impl::apply(y, value_bind(f, x.data_.dbl));
      case time_range_type:
        return visit_impl::apply(y, value_bind(f, x.data_.range));
      case time_point_type:
        return visit_impl::apply(y, value_bind(f, x.data_.time));
      case string_type:
        return visit_impl::apply(y, value_bind(f, x.data_.str));
      case regex_type:
        return visit_impl::apply(y, value_bind(f, *x.data_.rx));
      case vector_type:
        return visit_impl::apply(y, value_bind(f, *x.data_.vec));
      case set_type:
        return visit_impl::apply(y, value_bind(f, *x.data_.st));
      case table_type:
        return visit_impl::apply(y, value_bind(f, *x.data_.tbl));
      case record_type:
        return visit_impl::apply(y, value_bind(f, *x.data_.rec));
      case address_type:
        return visit_impl::apply(y, value_bind(f, x.data_.addr));
      case prefix_type:
        return visit_impl::apply(y, value_bind(f, x.data_.pfx));
      case port_type:
        return visit_impl::apply(y, value_bind(f, x.data_.prt));
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
    return nullptr;
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

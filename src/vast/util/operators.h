#ifndef VAST_UTIL_OPERATORS_H
#define VAST_UTIL_OPERATORS_H

#include "vast/traits.h"

namespace vast {
namespace util {

template <typename T, typename U = T>
struct equality_comparable
{
  friend bool operator!=(T const& x, T const& y)
  {
    return ! (x == y);
  }

  // FIXME: Clang spit out an error with this definition:
  //
  //  use of overloaded operator '!=' is ambiguous
  //
  // For now we just comment this extra ability to perform inequality
  // comparisons with different types.
  //
  //template <typename V = U>
  //friend DisableIf<std::is_same<V, T>, bool>
  //operator!=(T const& x, V const& y)
  //{
  //  return ! (x == y);
  //}
};

template <typename T, typename U = T>
struct less_than_comparable
{
  friend bool operator>(T const& x, T const& y)
  {
    return y < x;
  }

  friend bool operator<=(T const& x, T const& y)
  {
    return ! (y < x);
  }

  friend bool operator>=(T const& x, T const& y)
  {
    return ! (x < y);
  }

  template <typename V>
  friend DisableIf<std::is_same<V, T>, bool>
  operator>(T const& x, V const& y)
  {
    return y < x;
  }

  template <typename V>
  friend DisableIf<std::is_same<V, T>, bool>
  operator<=(T const& x, V const& y)
  {
    return ! (y < x);
  }

  template <typename V>
  friend DisableIf<std::is_same<V, T>, bool>
  operator>=(V const& x, T const& y)
  {
    return ! (x < y);
  }
};

template <typename T, typename U = T>
struct partially_ordered
{
  friend bool operator>(T const& x, T const& y)
  {
    return y < x;
  }

  friend bool operator<=(T const& x, T const& y)
  {
    return x < y || x == y;
  }

  friend bool operator>=(T const& x, T const& y)
  {
    return y < x || x == y;
  }

  template <typename V>
  friend DisableIf<std::is_same<V, T>, bool>
  operator>(T const& x, V const& y)
  {
    return y < x;
  }

  template <typename V>
  friend DisableIf<std::is_same<V, T>, bool>
  operator<=(T const& x, V const& y)
  {
    return x < y || x == y;
  }

  template <typename V>
  friend DisableIf<std::is_same<V, T>, bool>
  operator>=(T const& x, V const& y)
  {
    return y < x || x == y;
  }
};

template <typename T, typename U = T>
struct totally_ordered : equality_comparable<T, U>, less_than_comparable<T, U>
{
};

#define VAST_BINARY_OPERATOR_NON_COMMUTATIVE(NAME, OP)                      \
template <typename T, typename U = T>                                       \
struct NAME                                                                 \
{                                                                           \
  friend T operator OP(T const& x, T const& y)                              \
  {                                                                         \
     T t(x);                                                                \
     t OP##= y;                                                             \
     return t;                                                              \
  }                                                                         \
                                                                            \
  template <typename V = U>                                                 \
  friend DisableIf<std::is_same<V, T>, T>                                   \
  operator OP(T const& x, V const& y)                                       \
  {                                                                         \
     T t(x);                                                                \
     t OP##= y;                                                             \
     return t;                                                              \
  }                                                                         \
};

#define VAST_BINARY_OPERATOR_COMMUTATIVE(NAME, OP)                          \
template <typename T, typename U = T>                                       \
struct NAME                                                                 \
{                                                                           \
  friend T operator OP(T const& x, T const& y)                              \
  {                                                                         \
     T t(x);                                                                \
     t OP##= y;                                                             \
     return t;                                                              \
  }                                                                         \
                                                                            \
  template <typename V = U>                                                 \
  friend DisableIf<std::is_same<V, T>, T>                                   \
  operator OP(T const& x, V const& y)                                       \
  {                                                                         \
     T t(x);                                                                \
     t OP##= y;                                                             \
     return t;                                                              \
  }                                                                         \
};

VAST_BINARY_OPERATOR_COMMUTATIVE(addable, +);
VAST_BINARY_OPERATOR_COMMUTATIVE(multipliable, *);
VAST_BINARY_OPERATOR_NON_COMMUTATIVE(subtractable, -);
VAST_BINARY_OPERATOR_NON_COMMUTATIVE(dividable, /);
VAST_BINARY_OPERATOR_NON_COMMUTATIVE(modable, %);
VAST_BINARY_OPERATOR_COMMUTATIVE(xorable, ^);
VAST_BINARY_OPERATOR_COMMUTATIVE(andable, &);
VAST_BINARY_OPERATOR_COMMUTATIVE(orable, |);

#undef VAST_BINARY_OPERATOR_COMMUTATIVE

template <typename T, typename U = T>
struct additive
  : addable<T, U>, subtractable<T, U>
{};

template <typename T, typename U = T>
struct multiplicative
  : multipliable<T, U>, dividable<T, U>
{};

template <typename T, typename U = T>
struct integer_multiplicative
  : multiplicative<T, U>, modable<T, U>
{};

template <typename T, typename U = T>
struct arithmetic
  : additive<T, U>, multiplicative<T, U>
{};

template <typename T, typename U = T>
struct integer_arithmetic
  : additive<T, U>, integer_multiplicative<T, U>
{};

template <typename T, typename U = T>
struct bitwise
  : andable<T, U>, orable<T, U>, xorable<T, U>
{};

} // namespace util
} // namespace vast

#endif

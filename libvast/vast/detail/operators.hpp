#ifndef VAST_DETAIL_OPERATORS_HPP
#define VAST_DETAIL_OPERATORS_HPP

namespace vast {
namespace detail {

template <typename T, typename U = T>
struct equality_comparable {
  friend bool operator!=(T const& x, U const& y) {
    return !(x == y);
  }
};

template <typename T, typename U = T>
struct less_than_comparable {
  friend bool operator>(T const& x, U const& y) {
    return y < x;
  }

  friend bool operator<=(T const& x, U const& y) {
    return !(y < x);
  }

  friend bool operator>=(T const& x, U const& y) {
    return !(x < y);
  }
};

template <typename T, typename U = T>
struct partially_ordered {
  friend bool operator>(T const& x, U const& y) {
    return y < x;
  }

  friend bool operator<=(T const& x, U const& y) {
    return x < y || x == y;
  }

  friend bool operator>=(T const& x, U const& y) {
    return y < x || x == y;
  }
};

template <typename T, typename U = T>
struct totally_ordered : equality_comparable<T, U>,
                         less_than_comparable<T, U> {};

#define VAST_BINARY_OPERATOR_NON_COMMUTATIVE(NAME, OP)                         \
  template <typename T, typename U = T>                                        \
  struct NAME {                                                                \
    friend T operator OP(T const& x, U const& y) {                             \
      T t(x);                                                                  \
      t OP## = y;                                                              \
      return t;                                                                \
    }                                                                          \
  };

#define VAST_BINARY_OPERATOR_COMMUTATIVE(NAME, OP)                             \
  template <typename T, typename U = T>                                        \
  struct NAME {                                                                \
    friend T operator OP(T const& x, U const& y) {                             \
      T t(x);                                                                  \
      t OP## = y;                                                              \
      return t;                                                                \
    }                                                                          \
  };

VAST_BINARY_OPERATOR_COMMUTATIVE(addable, +)
VAST_BINARY_OPERATOR_COMMUTATIVE(multipliable, *)
VAST_BINARY_OPERATOR_NON_COMMUTATIVE(subtractable, -)
VAST_BINARY_OPERATOR_NON_COMMUTATIVE(dividable, / )
VAST_BINARY_OPERATOR_NON_COMMUTATIVE(modable, % )
VAST_BINARY_OPERATOR_COMMUTATIVE(xorable, ^)
VAST_BINARY_OPERATOR_COMMUTATIVE(andable, &)
VAST_BINARY_OPERATOR_COMMUTATIVE(orable, | )

#undef VAST_BINARY_OPERATOR_COMMUTATIVE

template <typename T, typename U = T>
struct additive : addable<T, U>, subtractable<T, U> {};

template <typename T, typename U = T>
struct multiplicative : multipliable<T, U>, dividable<T, U> {};

template <typename T, typename U = T>
struct integer_multiplicative : multiplicative<T, U>, modable<T, U> {};

template <typename T, typename U = T>
struct arithmetic : additive<T, U>, multiplicative<T, U> {};

template <typename T, typename U = T>
struct integer_arithmetic : additive<T, U>, integer_multiplicative<T, U> {};

template <typename T, typename U = T>
struct bitwise : andable<T, U>, orable<T, U>, xorable<T, U> {};

} // namespace detail
} // namespace vast

#endif // VAST_DETAIL_OPERATORS_HPP

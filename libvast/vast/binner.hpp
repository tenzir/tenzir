#ifndef VAST_BINNER_HPP
#define VAST_BINNER_HPP

#include <cmath>

#include <algorithm>
#include <type_traits>

#include "vast/util/math.hpp"

namespace vast {

/// A binning policy which computes the identity function.
struct identity_binner {
  template <typename T>
  static T bin(T x) {
    return x;
  }
};

/// A binning policy with fixed-size buckets.
template <size_t Exp>
struct decimal_binner {
  static constexpr double log10_2 = 0.3010299956639811980175;
  static constexpr uint64_t bucket_size = util::pow<Exp>(10ull);
  static constexpr uint64_t digits10 = Exp;
  static constexpr uint64_t digits2 = digits10 / log10_2 + 1;

  template <typename T>
  static auto bin(T x) -> std::enable_if_t<std::is_integral<T>{}, T> {
    return x / bucket_size;
  }

  template <typename T>
  static auto bin(T x) -> std::enable_if_t<std::is_floating_point<T>{}, T> {
    return std::round(x / bucket_size);
  }
};

template <size_t E>
constexpr uint64_t decimal_binner<E>::bucket_size;

template <size_t E>
constexpr uint64_t decimal_binner<E>::digits2;

/// A binning policy that reduces values to a given precision.
/// Integral types are truncated and fractional types are rounded.
/// @tparam IntegralDigits The number of positive decimal digits. For example,
///                        3 digits means that the largest value is 10^3.
/// @tparam FractionalDigits The number of negative decimal digits.
template <size_t IntegralDigits, size_t FractionalDigits = 0>
struct precision_binner {
  static constexpr uint64_t integral10 = IntegralDigits;
  static constexpr uint64_t fractional10 = FractionalDigits;
  static constexpr uint64_t integral_max = util::pow<integral10>(10ull);
  static constexpr uint64_t fractional_max = util::pow<fractional10>(10ull);
  static constexpr uint64_t digits10 = integral10 + fractional10;
  static constexpr double log10_2 = 0.3010299956639811980175;
  static constexpr uint64_t digits2 = digits10 / log10_2 + 1;

  template <typename T>
  static auto bin(T x) -> std::enable_if_t<std::is_integral<T>{}, T> {
    return std::min(x, integral_max);
  }

  template <typename T>
  static auto bin(T x) -> std::enable_if_t<std::is_floating_point<T>{}, T> {
    T i;
    auto f = std::modf(x, &i);
    auto negative = std::signbit(x);
    if (negative && -i >= static_cast<double>(integral_max))
      return -static_cast<double>(integral_max); // -Inf
    if (!negative && i >= static_cast<double>(integral_max))
      return integral_max; // +Inf
    f = std::round(f * fractional_max) / fractional_max;
    return i + f;
  }
};

template <size_t P, size_t N>
constexpr uint64_t precision_binner<P, N>::integral_max;

template <size_t P, size_t N>
constexpr uint64_t precision_binner<P, N>::fractional_max;

template <size_t P, size_t N>
constexpr uint64_t precision_binner<P, N>::digits10;

template <size_t P, size_t N>
constexpr uint64_t precision_binner<P, N>::digits2;

namespace detail {

template <typename T>
struct is_identity_binner : std::false_type {};

template <>
struct is_identity_binner<identity_binner> : std::true_type {};

template <typename T>
struct is_decimal_binner : std::false_type {};

template <size_t E>
struct is_decimal_binner<decimal_binner<E>> : std::true_type {};

template <typename T>
struct is_precision_binner : std::false_type {};

template <size_t P, size_t N>
struct is_precision_binner<precision_binner<P, N>> : std::true_type {};

} // namespace detail
} // namespace vast

#endif

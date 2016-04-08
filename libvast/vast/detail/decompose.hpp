#ifndef VAST_DETAIL_DECOMPOSE_HPP
#define VAST_DETAIL_DECOMPOSE_HPP

#include <cstdint>
#include <array>

namespace vast {
namespace detail {

/// Decomposes a value into a vector of values according to a given base.
/// @param x The value to decompose.
/// @param base The base vector used to decompose *x*.
/// @returns The coefficients of *x* for *base*.
/// @pre *base* is well-defined.
template <typename T, size_t N>
std::array<T, N> decompose(T x, std::array<size_t, N> const& base) {
  static_assert(N > 0, "need at least one component");
  std::array<T, N> result;
  for (auto i = 0u; i < N; ++i) {
    result[i] = x % base[i];
    x /= base[i];
  }
  return result;
}

template <typename T, size_t N>
T compose(std::array<T, N> const& xs, std::array<size_t, N> const& base) {
  static_assert(N > 0, "need at least one component");
  auto result = T{0};
  auto b = T{1};
  for (auto i = 0u; i < N; ++i) {
    result += xs[i] * b;
    b *= base[i];
  }
  return result;
}

} // namespace detail
} // namespace vast

#endif

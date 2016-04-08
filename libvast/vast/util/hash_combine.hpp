#ifndef VAST_UTIL_HASH_COMBINE_HPP
#define VAST_UTIL_HASH_COMBINE_HPP

#include <cstddef>
#include <type_traits>

namespace vast {
namespace util {

/// Hashes 128-bit input down to 64 bits.
/// @note Taken from Google's CityHash; licence: MIT
inline uint64_t hash_128_to_64(uint64_t upper, uint64_t lower) {
  static uint64_t const k_mul = 0x9ddfea08eb382d69ull;
  auto a = (lower ^ upper) * k_mul;
  a ^= (a >> 47);
  auto b = (upper ^ a) * k_mul;
  b ^= (b >> 47);
  b *= k_mul;
  return b;
}

/// Hashes 64-bit input down to 32 bits.
/// @note This is a slight modification of Thomas Wang's function.
/// @see https://gist.github.com/badboy/6267743.
inline uint32_t hash_64_to_32(uint64_t x) {
  x = (~x) + (x << 18);
  x ^= x >> 31;
  x *= 21;
  x ^= x >> 11;
  x += x << 6;
  x ^= x >> 22;
  return static_cast<uint32_t>(x);
}

template <typename T>
auto hash_combine(T const& x) {
  return std::hash<T>{}(x);
}

/// Combines multiple hash digests.
/// @param x The first value to hash.
/// @param xs The remaining values to hash and combine with *x*.
/// @returns The combined hash value of *x* and all *xs*.
template <typename T, typename... Ts>
std::enable_if_t<(sizeof(T*) == 4), size_t>
hash_combine(T const& x, Ts const&... xs) {
  // We use hash_combine as proposed in N3876 (or used in Boost).
  auto seed = hash_combine(xs...);
  seed ^= hash_combine(x) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
  return seed;
}

template <typename T, typename... Ts>
std::enable_if_t<(sizeof(T*) == 8), size_t>
hash_combine(T const& x, Ts const&... xs) {
  return hash_128_to_64(hash_combine(x), hash_combine(xs...));
}

} // namespace util
} // namespace vast

#endif

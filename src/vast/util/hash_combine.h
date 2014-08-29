#ifndef VAST_UTIL_HASH_COMBINE_H
#define VAST_UTIL_HASH_COMBINE_H

#include <cstddef>

namespace vast {
namespace util {

// Hashes 128-bit input down to 64 bits.
// @note Taken from Google's CityHash; licence: MIT
inline size_t hash_128_to_64(size_t upper, size_t lower)
{
  static size_t const k_mul = 0x9ddfea08eb382d69ull;

  auto a = (lower ^ upper) * k_mul;
  a ^= (a >> 47);

  auto b = (upper ^ a) * k_mul;
  b ^= (b >> 47);
  b *= k_mul;

  return b;
}

/// Convenience function to combine multiple hash digests.
/// @param x The value to hash.
/// @returns The hash value of *x*.
template <typename T>
size_t hash_combine(T const& x)
{
  return std::hash<T>{}(x);
}

/// Combines multiple hash digests.
/// @param x The first value to hash.
/// @param xs The remaining values to hash and combine with *x*.
/// @returns The combined hash value of *x* and all *xs*.
template <typename T, typename... Ts>
size_t hash_combine(T const& x, Ts const&... xs)
{
  return hash_128_to_64(hash_combine(x), hash_combine(xs...));
}

} // namespace util
} // namespace vast

#endif

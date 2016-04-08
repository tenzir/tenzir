#ifndef VAST_UTIL_HASH_MURMUR_HPP
#define VAST_UTIL_HASH_MURMUR_HPP

#include <array>
#include <limits>

#include "vast/util/assert.hpp"
#include "vast/util/hash.hpp"

namespace vast {
namespace util {

namespace detail {

void murmur3_x86_32(const void* key, int len, uint32_t seed, void* out);
void murmur3_x86_128(const void* key, int len, uint32_t seed, void* out);
void murmur3_x64_128(const void* key, int len, uint32_t seed, void* out);

template <size_t N>
std::enable_if_t<(N== 32)>
murmur3(void const* key, int len, uint32_t seed, void* out) {
  murmur3_x86_32(key, len, seed, out);
}

template <size_t N>
std::enable_if_t<(N == 128 && sizeof(void*) == 4)>
murmur3(void const* key, int len, uint32_t seed, void* out) {
  murmur3_x86_128(key, len, seed, out);
}

template <size_t N>
std::enable_if_t<(N == 128 && sizeof(void*) == 8)>
murmur3(void const* key, int len, uint32_t seed, void* out) {
  murmur3_x64_128(key, len, seed, out);
}

} // namespace detail

/// The [Murmur3](https://code.google.com/p/smhasher) algorithm.
/// @tparam Bits The number of output bits. Allowed values: 32 or 128.
template <size_t Bits = 32>
class murmur3 : public hash<murmur3<Bits>> {
public:
  static_assert(Bits == 32 || Bits == 128,
                "murmur3 only supports 32 or 128 bit output");

  using digest_type =
    typename std::conditional<
      Bits == 32, uint32_t, std::array<uint64_t, 2>
    >::type;

  static digest_type value(void const* x, size_t n, uint32_t seed = 0) {
    VAST_ASSERT(n <= std::numeric_limits<int>::max());
    digest_type d;
    detail::murmur3<Bits>(x, static_cast<int>(n), seed, &d);
    return d;
  }
};

} // namespace util
} // namespace vast

#endif

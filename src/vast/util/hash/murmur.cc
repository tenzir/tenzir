#include "murmur/murmur3.h"

#include "vast/util/hash/murmur.h"

namespace vast {
namespace util {
namespace detail {

void murmur3_x86_32(const void* key, int len, uint32_t seed, void* out) {
  MurmurHash3_x86_32(key, len, seed, out);
}

void murmur3_x86_128(const void* key, int len, uint32_t seed, void* out) {
  MurmurHash3_x86_128(key, len, seed, out);
}

void murmur3_x64_128(const void* key, int len, uint32_t seed, void* out) {
  MurmurHash3_x64_128(key, len, seed, out);
}

} // namespace detail
} // namespace util
} // namespace vast

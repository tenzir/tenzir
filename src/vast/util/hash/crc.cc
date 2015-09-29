#include "crc/crc32.h"

#include "vast/util/hash/crc.h"

namespace vast {
namespace util {

crc32::crc32(uint32_t seed) : digest_{seed} { }

crc32::digest_type crc32::value(void const* x, size_t n, uint32_t seed) {
  digest_type d;
  ::crc32(x, static_cast<int>(n), seed, &d);
  return d;
}

bool crc32::update(void const* x, size_t n) {
  ::crc32(x, static_cast<int>(n), digest_, &digest_);
  return true;
}

crc32::digest_type crc32::compute() {
  return digest_;
}

} // namespace util
} // namespace vast

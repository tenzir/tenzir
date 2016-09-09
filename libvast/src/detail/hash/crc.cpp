#include "crc/crc32.h"

#include "vast/detail/assert.hpp"
#include "vast/detail/hash/crc.hpp"

namespace vast {
namespace detail {

crc32::crc32(uint32_t seed) : digest_{seed} { }

void crc32::operator()(void const* x, size_t n) {
  VAST_ASSERT(n <= (1u << 31) - 1);
  ::crc32(x, static_cast<int>(n), digest_, &digest_);
}

crc32::operator result_type() const {
  return digest_;
}

} // namespace detail
} // namespace vast

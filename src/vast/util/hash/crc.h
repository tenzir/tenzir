#ifndef VAST_UTIL_HASH_CRC_H
#define VAST_UTIL_HASH_CRC_H

#include <cassert>
#include "vast/util/hash.h"

namespace vast {
namespace util {
namespace detail {

void crc32(void const* key, int len, uint32_t seed, void* out);

} // namespace detail

/// The [CRC32](http://en.wikipedia.org/wiki/Cyclic_redundancy_check) algorithm.
class crc32 : public hash<crc32>
{
  friend hash<crc32>;

public:
  using digest_type = uint32_t;

  crc32(uint32_t seed = 0)
    : digest_{seed}
  {
  }

private:
  static digest_type value(void const* x, size_t n, uint32_t seed = 0)
  {
    digest_type d;
    detail::crc32(x, static_cast<int>(n), seed, &d);
    return d;
  }

  /// @pre `n <= max`
  bool update(void const* x, size_t n)
  {
    detail::crc32(x, static_cast<int>(n), digest_, &digest_);
    return true;
  }

  digest_type compute()
  {
    return digest_;
  }

  digest_type digest_;
};

} // namespace util
} // namespace vast

#endif

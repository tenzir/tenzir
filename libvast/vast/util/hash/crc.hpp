#ifndef VAST_UTIL_HASH_CRC_HPP
#define VAST_UTIL_HASH_CRC_HPP

#include <cstdint>

#include "vast/util/hash.hpp"

namespace vast {
namespace util {

/// The [CRC32](http://en.wikipedia.org/wiki/Cyclic_redundancy_check) algorithm.
class crc32 : public hash<crc32> {
  friend hash<crc32>;

public:
  using digest_type = uint32_t;

  crc32(uint32_t seed = 0);

private:
  static digest_type value(void const* x, size_t n, uint32_t seed = 0);

  bool update(void const* x, size_t n);

  digest_type compute();

  digest_type digest_;
};

} // namespace util
} // namespace vast

#endif

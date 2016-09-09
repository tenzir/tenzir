#ifndef VAST_DETAIL_HASH_CRC_HPP
#define VAST_DETAIL_HASH_CRC_HPP

#include <cstdint>

#include "vast/detail/endian.hpp"

namespace vast {
namespace detail {

/// The [CRC32](http://en.wikipedia.org/wiki/Cyclic_redundancy_check) algorithm.
class crc32 {
public:
  static constexpr endianness endian = host_endian;
  using result_type = uint32_t;

  crc32(uint32_t seed = 0);

  void operator()(void const* x, size_t n);

  operator result_type() const;

  template <class Inspector>
  friend auto inspect(Inspector& f, crc32& crc) {
    return f(crc.digest_);
  }

private:
  result_type digest_;
};

} // namespace detail
} // namespace vast

#endif

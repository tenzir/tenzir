#ifndef VAST_UTIL_CRC_H
#define VAST_UTIL_CRC_H

#include <boost/crc.hpp>

namespace vast {
namespace util {

typedef boost::crc_32_type crc32;

} // namespace util
} // namespace vast

#endif

#ifndef VAST_IO_COMPRESSION_H
#define VAST_IO_COMPRESSION_H

#include <cstdint>

#include "vast/config.h"

namespace vast {
namespace io {

enum compression : uint8_t {
  null      = 0,
  automatic = 1,  // TODO: implement automatic detection of the compression format.
  lz4       = 2,
#ifdef VAST_HAVE_SNAPPY
  snappy    = 3,
#endif // VAST_HAVE_SNAPPY
};

} // namespace io
} // namespace vast

#endif

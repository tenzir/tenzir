#include "vast/io/device.hpp"

#include <algorithm>

namespace vast {
namespace io {

bool input_device::skip(size_t bytes, size_t* skipped) {
  char buf[4096];
  size_t got = 0;
  size_t total = 0;
  while (total < bytes) {
    auto n = std::min(bytes - total, sizeof(buf));
    if (!read(buf, n, &got)) {
      if (skipped)
        *skipped += total;
      return false;
    }
    total += got;
  }
  if (skipped)
    *skipped += total;
  return true;
}

} // namespace io
} // namespace vast

#include <unistd.h>

#include <cstdio>

#include "vast/util/assert.h"
#include "vast/util/fdinbuf.h"

namespace vast {
namespace util {

fdinbuf::fdinbuf(int fd, size_t buffer_size)
  : fd_{fd},
    buffer_(buffer_size) {
  VAST_ASSERT(buffer_size > putback_area_size);
  setg(buffer_.data() + putback_area_size,  // beginning of putback area
       buffer_.data() + putback_area_size,  // read position
       buffer_.data() + putback_area_size); // end position
}

fdinbuf::int_type fdinbuf::underflow() {
  // Is the read position before the buffer end?
  if (gptr() < egptr())
    return traits_type::to_int_type(*gptr());
  // Process putback area.
  int num_putback = gptr() - eback();
  if (static_cast<size_t>(num_putback) > putback_area_size)
    num_putback = putback_area_size;
  // Copy over previously read characters from the putback area.
  std::memmove(buffer_.data() + (putback_area_size - num_putback),
               gptr() - num_putback, num_putback);
  // Read new characters.
  auto n = ::read(fd_, buffer_.data() + putback_area_size,
                  buffer_.size() - putback_area_size);
  if (n == 0)
    return traits_type::eof();
  if (n < 0) {
    // Only a return value of 0 indicates EOF for read(2). Any value < 0
    // represents an error. In §27.6.3.4.3/12, the standard says:
    //
    //     If the pending sequence is empty, either gptr() is null or gptr()
    //     and egptr() are set to the same non-null pointer value.
    //
    // We do the latter.
    setg(buffer_.data() + (putback_area_size - num_putback),
         buffer_.data() + putback_area_size,
         buffer_.data() + putback_area_size);
    return traits_type::eof();
  }
  // Reset buffer pointers.
  setg(buffer_.data() + (putback_area_size - num_putback),
       buffer_.data() + putback_area_size,
       buffer_.data() + putback_area_size + n);
  // Return next character.
  return traits_type::to_int_type(*gptr());
}

} // namespace util
} // namespace vast

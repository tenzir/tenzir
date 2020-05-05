/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#include "vast/detail/fdinbuf.hpp"

#include "vast/detail/assert.hpp"

#include <cstdio>
#include <cstring>
#include <unistd.h>

#include <sys/poll.h>

namespace vast {
namespace detail {

fdinbuf::fdinbuf(int fd, size_t buffer_size)
  : fd_{fd}, buffer_(buffer_size), timeout_fail_(false) {
  VAST_ASSERT(buffer_size > putback_area_size);
  setg(buffer_.data() + putback_area_size,  // beginning of putback area
       buffer_.data() + putback_area_size,  // read position
       buffer_.data() + putback_area_size); // end position
}

// Note that to implement non-blocking reads we cannot simply switch the
// file descriptor to non-blocking mode because it might refer to stdin, and
// putting stdin into non-blocking mode will automatically do the same for stdout.
std::optional<std::chrono::milliseconds>& fdinbuf::read_timeout() {
  return read_timeout_;
}

bool fdinbuf::timed_out() const {
  return timeout_fail_;
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
  // Ensure we have data to read if a read timeout was set.
  if (read_timeout_) {
    struct pollfd pfd {
      fd_, POLLIN, 0
    };
    int res;
    while ((res = ::poll(&pfd, 1, read_timeout_->count())) == -1)
      if (errno != EINTR)
        break;
    if (res == 0)
      timeout_fail_ = true;
    // Poll failure (memory/file descriptor limit exceeded; or no readable data)
    if (res < 1 || !((pfd.revents & POLLIN) || (pfd.revents & POLLHUP)))
      return traits_type::eof();
  }
  timeout_fail_ = false;
  // Read new characters.
  ssize_t n = ::read(fd_, buffer_.data() + putback_area_size,
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

} // namespace detail
} // namespace vast

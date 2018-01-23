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

#include <unistd.h>

#include <cstdio>

#include "vast/detail/fdoutbuf.hpp"

namespace vast {
namespace detail {

fdoutbuf::fdoutbuf(int fd) : fd_{fd} {
}

fdoutbuf::int_type fdoutbuf::overflow(int_type c) {
  if (c != EOF) {
    char z = c;
    if (::write(fd_, &z, 1) != 1)
      return EOF;
  }
  return c;
}

std::streamsize fdoutbuf::xsputn(const char* s, std::streamsize n) {
  return ::write(fd_, s, n);
}

} // namespace detail
} // namespace vast

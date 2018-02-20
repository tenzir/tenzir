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

#ifndef VAST_DETAIL_FDOUTBUF_HPP
#define VAST_DETAIL_FDOUTBUF_HPP

#include <streambuf>

namespace vast::detail {

/// A streambuffer that proxies writes to an underlying POSIX file descriptor.
class fdoutbuf : public std::streambuf {
public:
  /// Constructs an output streambuffer from a POSIX file descriptor.
  /// @param fd The file descriptor to construct the streambuffer for.
  fdoutbuf(int fd);

protected:
  int_type overflow(int_type c) override;
  std::streamsize xsputn(const char* s, std::streamsize n) override;

private:
  int fd_;
};

} // namespace vast::detail

#endif

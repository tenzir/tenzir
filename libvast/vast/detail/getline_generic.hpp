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

#pragma once

#include <istream>
#include <string>

// The code in this file is based on a stackoverflow
// post by user763305 et al., originally posted at
// https://stackoverflow.com/a/6089413/92560.

namespace vast::detail {

/// Get one line from the istream `is`, ignoring the current platform
/// delimiter and recognizing any of `\n`, `\r\n` and `\r` instead.
inline std::istream& getline_generic(std::istream& is, std::string& t) {
  t.clear();
  // The characters in the stream are read one-by-one using a std::streambuf.
  // That is faster than reading them one-by-one using the std::istream.
  // Code that uses streambuf this way must be guarded by a sentry object.
  std::istream::sentry sentry(is, true);
  std::streambuf* sb = is.rdbuf();
  for (;;) {
    int c = sb->sbumpc();
    switch (c) {
      case '\n':
        return is;
      case '\r':
        if (sb->sgetc() == '\n')
          sb->sbumpc();
        return is;
      case std::streambuf::traits_type::eof():
        // Also handle the case when the last line has no line ending
        if (t.empty())
          is.setstate(std::ios::eofbit);
        return is;
      default:
        t += static_cast<char>(c);
    }
  }
}

} // namespace vast::detail

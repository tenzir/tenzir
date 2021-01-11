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

// This file comes from a 3rd party and has been adapted to fit into the VAST
// code base. Details about the original code:
//
// - Site:       https://stackoverflow.com/a/6089413/92560
// - Author:     user763305 et al.
// - License:    CC-BY-SA

#pragma once

#include <istream>
#include <string>

namespace vast::detail {

/// Get one line from the istream `is`, ignoring the current platform
/// delimiter and recognizing any of `\n`, `\r\n` and `\r` instead.
inline std::istream& getline_generic(std::istream& is, std::string& t) {
  // The characters in the stream are read one-by-one using a std::streambuf.
  // That is faster than reading them one-by-one using the std::istream.
  // Code that uses streambuf this way must be guarded by a sentry object.
  std::istream::sentry sentry(is, true);
  if (!sentry)
    return is;
  size_t n = 0;
  std::streambuf* sb = is.rdbuf();
  while (t.size() < t.max_size()) {
    int c = sb->sbumpc();
    switch (c) {
      case '\n':
        return is;
      case '\r':
        if (sb->sgetc() == '\n')
          sb->sbumpc();
        return is;
      case std::streambuf::traits_type::eof():
        is.setstate(std::ios::eofbit);
        // If `std::getline` extracts no characters, failbit is set.
        // (21.4.8.9/7.9 [string.io])
        if (n == 0)
          is.setstate(std::ios::failbit);
        return is;
      default:
        t += static_cast<char>(c);
        n++;
    }
  }
  // After `str.max_size()` are stored by `std::getline`, failbit is set.
  // (21.4.8.9/7.7.1 [string.io])
  is.setstate(std::ios::failbit);
  return is;
}

} // namespace vast::detail

//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/detail/fdoutbuf.hpp"

#include <cstdio>
#include <unistd.h>

namespace tenzir {
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
} // namespace tenzir

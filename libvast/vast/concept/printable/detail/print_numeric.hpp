//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/detail/coding.hpp"

#include <algorithm>
#include <limits>
#include <type_traits>

namespace vast::detail {

template <class Iterator, class T>
size_t print_numeric(Iterator& out, T x) {
  static_assert(std::is_integral<T>{}, "T must be an integral type");
  if (x == 0) {
    *out++ = '0';
    return 1;
  }
  char buf[std::numeric_limits<T>::digits10 + 1];
  auto ptr = buf;
  while (x > 0) {
    *ptr++ = byte_to_char(x % 10);
    x /= 10;
  }
  out = std::reverse_copy(buf, ptr, out);
  return ptr - buf;
}

} // namespace vast::detail

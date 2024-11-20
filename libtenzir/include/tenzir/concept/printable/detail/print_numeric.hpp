//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/concepts.hpp"
#include "tenzir/detail/coding.hpp"

#include <algorithm>
#include <limits>
#include <type_traits>

namespace tenzir::detail {

template <class Iterator, std::integral T>
size_t print_numeric(Iterator& out, T x) {
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

} // namespace tenzir::detail

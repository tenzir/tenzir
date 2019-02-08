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

#include <algorithm>
#include <limits>
#include <type_traits>

#include "vast/detail/coding.hpp"

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

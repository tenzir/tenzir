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

#include "vast/byte.hpp"
#include "vast/span.hpp"

#include <type_traits>
#include <vector>

namespace vast {

template <class T>
span<const byte> as_bytes(const std::vector<T>& xs) noexcept {
  static_assert(std::is_integral_v<T>, "byte span requires integral types");
  auto data = reinterpret_cast<const byte*>(xs.data());
  return {data, xs.size() * sizeof(T)};
}

template <class T>
span<byte> as_writeable_bytes(std::vector<T>& xs) noexcept {
  static_assert(std::is_integral_v<T>, "byte span requires integral types");
  auto data = reinterpret_cast<byte*>(xs.data());
  return {data, xs.size() * sizeof(T)};
}

} // namespace vast

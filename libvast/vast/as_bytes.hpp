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
#include "vast/detail/type_traits.hpp"
#include "vast/span.hpp"

#include <type_traits>

namespace vast {

template <class Buffer,
          class = std::enable_if_t<detail::is_byte_container_v<Buffer>>>
constexpr span<const byte> as_bytes(const Buffer& xs) noexcept {
  const auto data = reinterpret_cast<const byte*>(std::data(xs));
  return {data, std::size(xs)};
}

template <class Buffer,
          class = std::enable_if_t<detail::is_byte_container_v<Buffer>>>
constexpr span<byte> as_writeable_bytes(Buffer& xs) noexcept {
  const auto data = reinterpret_cast<byte*>(std::data(xs));
  return {data, std::size(xs)};
}

} // namespace vast

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

#include "vast/as_bytes.hpp"
#include "vast/detail/type_traits.hpp"
#include "vast/span.hpp"

#include <caf/binary_deserializer.hpp>
#include <caf/error.hpp>

namespace vast::detail {

/// Deserializes a sequence of objects from a byte buffer.
/// @param buffer The vector of bytes to read from.
/// @param xs The object to deserialize.
/// @returns The status of the operation.
/// @relates detail::serialize
template <class Buffer, class... Ts,
          class = std::enable_if_t<detail::is_byte_container_v<Buffer>>>
caf::error deserialize(const Buffer& buffer, Ts&&... xs) {
  auto bytes = as_bytes(buffer);
  auto data = reinterpret_cast<const char*>(bytes.data());
  caf::binary_deserializer deserializer{nullptr, data, bytes.size()};
  return deserializer(xs...);
}

} // namespace vast::detail

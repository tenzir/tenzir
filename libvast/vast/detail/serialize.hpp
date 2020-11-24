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

#include <caf/binary_serializer.hpp>
#include <caf/error.hpp>

#include <vector>

namespace vast::detail {

/// Serializes a sequence of objects into a byte buffer.
/// @param buffer The vector of bytes to write into.
/// @param xs The objects to serialize.
/// @returns The status of the operation.
/// @relates detail::deserialize
template <class Byte, class... Ts>
caf::error serialize(std::vector<Byte>& buffer, Ts&&... xs) {
  static_assert(sizeof(Byte) == 1, "can only serialize into byte vectors");
  caf::binary_serializer serializer{nullptr, buffer};
  return serializer(xs...);
}

} // namespace vast::detail

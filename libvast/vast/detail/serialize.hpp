// SPDX-FileCopyrightText: (c) 2020 Tenzir GmbH <info@tenzir.com>
// SPDX-License-Identifier: BSD-3-Clause

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

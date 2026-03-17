//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2020 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/detail/inspection_common.hpp"

#include <caf/binary_serializer.hpp>
#include <caf/error.hpp>

#include <vector>

namespace tenzir::detail {

/// Serializes a sequence of objects into a byte buffer.
/// @param buffer The vector of bytes to write into.
/// @param xs The objects to serialize.
/// @returns The status of the operation.
/// @relates detail::deserialize
template <class... Ts>
auto serialize(caf::byte_buffer& buffer, Ts&&... xs) {
  caf::binary_serializer serializer{buffer};
  return apply_all(serializer, std::forward<Ts>(xs)...);
}

} // namespace tenzir::detail

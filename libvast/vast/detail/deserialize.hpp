//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/as_bytes.hpp"
#include "vast/detail/concepts.hpp"
#include "vast/span.hpp"

#include <caf/binary_deserializer.hpp>
#include <caf/error.hpp>

#include <span>

namespace vast::detail {

/// Deserializes a sequence of objects from a byte buffer.
/// @param buffer The vector of bytes to read from.
/// @param xs The object to deserialize.
/// @returns The status of the operation.
/// @relates detail::serialize
template <detail::byte_container Buffer, class... Ts>
caf::error deserialize(const Buffer& buffer, Ts&&... xs) {
  auto bytes = as_bytes(buffer);
  auto data = reinterpret_cast<const char*>(bytes.data());
  caf::binary_deserializer deserializer{nullptr, data, bytes.size()};
  return deserializer(xs...);
}

} // namespace vast::detail

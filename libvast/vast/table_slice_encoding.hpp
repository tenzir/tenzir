//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include <string>

namespace vast {

/// The possible encodings of a table slice.
/// @note This encoding is unversioned. Newly created table slices are
/// guaranteed to use the newest vesion of the encoding, while deserialized
/// table slices may use an older version.
enum class table_slice_encoding : uint8_t {
  none,    ///< No data is encoded; the table slice is empty or invalid.
  arrow,   ///< The table slice is encoded using the Apache Arrow format.
  msgpack, ///< The table slice is encoded using the MessagePack format.
};

/// @relates table_slice_encoding
std::string to_string(table_slice_encoding encoding) noexcept;

} // namespace vast

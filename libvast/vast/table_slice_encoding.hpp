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

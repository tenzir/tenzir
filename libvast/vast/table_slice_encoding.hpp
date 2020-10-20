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

namespace vast {

/// Tag for the unversioned encoding of a table slice.
enum class table_slice_encoding : uint8_t {
  invalid, ///< An invalid table slice.
  msgpack, ///< A MessagePack-encoded table slice.
  arrow,   ///< An Arrow-encoded table slice.
  COUNT,   ///< Not an encoding; this must always be the last entry.
};

/// @returns A textual representation of the given encoding.
/// @param encoding The encoding.
const char* to_string(table_slice_encoding encoding);

} // namespace vast

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

#include <cstdint>
#include <cstddef>

#include "vast/config.hpp"

namespace vast {

/// A compression algorithm identifier.
enum class compression : int8_t {
  null      = 0,
  lz4       = 1,
#ifdef VAST_HAVE_SNAPPY
  snappy    = 2
#endif
};

/// The LZ4 compression algorithm.
namespace lz4 {

/// Returns an upper bound for the compressed output.
/// @param size The size of the uncompressed input.
size_t compress_bound(size_t size);

/// Compresses a contiguous byte sequence.
size_t compress(const char* in, size_t in_size, char* out, size_t out_size);

/// Uncompresses a contiguous byte sequence.
size_t uncompress(const char* in, size_t in_size, char* out, size_t out_size);

} // namespace lz4

#ifdef VAST_HAVE_SNAPPY
/// The Snappy compression algorithm.
namespace snappy {

/// Returns an upper bound for the compressed output.
/// @param size The size of the uncompressed input.
size_t compress_bound(size_t size);

/// Returns the size of the uncompressed output.
/// @param size The size of the uncompressed input.
size_t uncompress_bound(const char* data, size_t size);

/// Compresses a contiguous byte sequence.
size_t compress(const char* in, size_t in_size, char* out);

/// Uncompresses a contiguous byte sequence.
bool uncompress(const char* in, size_t in_size, char* out);

} // namespace snappy
#endif // VAST_SNAPPY

} // namespace vast


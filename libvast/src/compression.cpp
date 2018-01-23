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

#define LZ4_DISABLE_DEPRECATE_WARNINGS
#define LZ4_FORCE_INLINE
#include "lz4/lib/lz4.c"

#include "vast/compression.hpp"
#include "vast/die.hpp"

#ifdef VAST_HAVE_SNAPPY
#include <snappy.h>
#endif

namespace vast {
namespace lz4 {

size_t compress_bound(size_t size) {
  return LZ4_compressBound(size);
}

size_t compress(const char* in, size_t in_size, char* out, size_t out_size) {
  return LZ4_compress_default(in, out, in_size, out_size);
}

size_t uncompress(const char* in, size_t in_size, char* out, size_t out_size) {
  return LZ4_decompress_safe(in, out, static_cast<int>(in_size),
                             static_cast<int>(out_size));
}

} // namespace lz4

#ifdef VAST_HAVE_SNAPPY
namespace snappy {

size_t compress_bound(size_t size) {
  return ::snappy::MaxCompressedLength(size);
}

size_t uncompress_bound(const char* data, size_t size) {
  size_t n;
  if (! ::snappy::GetUncompressedLength(data, size, &n))
    return 0;
  return n;
}

size_t compress(const char* in, size_t in_size, char* out) {
  size_t n;
  ::snappy::RawCompress(in, in_size, out, &n);
  return n;
}

bool uncompress(const char* in, size_t in_size, char* out) {
  return ::snappy::RawUncompress(in, in_size, out);
}

} // namespace snappy
#endif // VAST_HAVE_SNAPPY

} // namespace vast

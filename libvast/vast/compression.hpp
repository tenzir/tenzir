#ifndef VAST_COMPRESSION_HPP
#define VAST_COMPRESSION_HPP

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
size_t compress(char const* in, size_t in_size, char* out, size_t out_size);

/// Uncompresses a contiguous byte sequence.
size_t uncompress(char const* in, size_t in_size, char* out, size_t out_size);

} // namespace lz4

#ifdef VAST_HAVE_SNAPPY
/// The Snappy compression algorithm.
namespace snappy {

/// Returns an upper bound for the compressed output.
/// @param size The size of the uncompressed input.
size_t compress_bound(size_t size);

/// Returns the size of the uncompressed output.
/// @param size The size of the uncompressed input.
size_t uncompress_bound(char const* data, size_t size);

/// Compresses a contiguous byte sequence.
size_t compress(char const* in, size_t in_size, char* out);

/// Uncompresses a contiguous byte sequence.
bool uncompress(char const* in, size_t in_size, char* out);

} // namespace snappy
#endif // VAST_SNAPPY

} // namespace vast

#endif

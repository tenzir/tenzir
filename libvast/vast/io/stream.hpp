#ifndef VAST_IO_STREAM_HPP
#define VAST_IO_STREAM_HPP

#include <cstddef>
#include <cstdint>
#include <utility>
#include "vast/config.hpp"
#include "vast/io/buffer.hpp"

namespace vast {
namespace io {

static size_t const default_block_size = 8 << 10;

/// An abstract input stream interface.
class input_stream {
public:
  virtual ~input_stream() = default;

  /// Retrieves a contiguous block of data from the stream.
  /// @returns A buffer filled with the next block of bytes or an invalid
  ///          buffer.
  buffer<void const> next_block();

  /// Retrieves a contiguous data buffer from the stream.
  /// @param data A result parameter that contains the beginning of the next
  ///             available data buffer *iff* next() returned `true`.
  /// @param size A result parameter that contains the size of *data* *iff*
  ///             next() returned `true`.
  /// @returns `true` if the input has still data available, and `false` if an
  /// error occurred or the input has no more data.
  virtual bool next(void const** data, size_t* size) = 0;

  /// Rewinds the stream position by a given number of bytes. Subsequent calls
  /// to next() then return previous data again.
  /// @param bytes The number of bytes to rewind the input stream.
  virtual void rewind(size_t bytes) = 0;

  /// Skips a given number of bytes.
  /// @param bytes The number of bytes to advance.
  /// @returns `true` if skipping was successful and `false` otherwise.
  virtual bool skip(size_t bytes) = 0;

  /// Retrieves the number of bytes this input stream processed.
  /// @returns The number of bytes this input stream processed.
  virtual uint64_t bytes() const = 0;

protected:
  input_stream() = default;
};

/// An abstract output stream interface.
class output_stream {
public:
  virtual ~output_stream() = default;

  /// Retrieves a contiguous block of data from the stream.
  /// @returns A buffer filled with the next block of bytes or an invalid
  ///          buffer.
  buffer<void> next_block();

  /// Retrieves a contiguous data buffer from the stream for write operations.
  /// @param data A result parameter that contains the beginning of the next
  ///             available data buffer *iff* next() returned `true`.
  /// @param size A result parameter that contains the size of *data* *iff*
  ///             next() returned `true`.
  /// @returns `true` if the output has a buffer available, and `false` if an
  /// error occurred.
  virtual bool next(void** data, size_t* size) = 0;

  /// Rewinds the stream position by a given number of bytes. Rewound bytes are
  /// not written into the stream. This is useful if the last buffer returned
  /// by next() is bigger than necessary.
  /// @param bytes The number of bytes to rewind the output stream.
  virtual void rewind(size_t bytes) = 0;

  /// If buffered, flushes the current state to the underlying device.
  /// @returns `true` If not flushable or if flushing succeeded, and `false`
  ///          if flushable but flushing failed.
  virtual bool flush();

  /// Retrieves the number of bytes this output stream processed.
  /// @returns The number of bytes this output stream processed.
  virtual uint64_t bytes() const = 0;

protected:
  output_stream() = default;
};

} // namespace io
} // namespace vast

#endif

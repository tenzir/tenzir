#ifndef VAST_IO_STREAM_H
#define VAST_IO_STREAM_H

#include <cstdint>
#include <utility>
#include "vast/config.h"

namespace vast {
namespace io {

static size_t const default_block_size = 8 << 10;

/// An abstract input stream interface.
class input_stream
{
public:
  /// Destroys an input stream.
#ifdef VAST_CLANG
  virtual ~input_stream() = default;
#else
  virtual ~input_stream() { };
#endif

  /// Retrieves a contiguous data buffer from the stream.
  ///
  /// @param data A result parameter that contains the beginning of the next
  /// available data buffer *iff* next() returned `true`.
  ///
  /// @param size A result parameter that contains the size of *data* *iff*
  /// next() returned `true`.
  ///
  /// @return `true` if the input has still data available, and `false` if an
  /// error occurred or the input has no more data.
  virtual bool next(void const** data, size_t* size) = 0;

  /// Rewinds the stream position by a given number of bytes. Subsequent calls
  /// to next() then return previous data again.
  ///
  /// @param bytes The number of bytes to rewind the input stream.
  virtual void rewind(size_t bytes) = 0;

  /// Skips a given number of bytes.
  /// @param bytes The number of bytes to advance.
  /// @return `true` if skipping was successful and `false` otherwise.
  virtual bool skip(size_t bytes) = 0;

  /// Retrieves the number of bytes this input stream processed.
  /// @return The number of bytes this input stream processed.
  virtual uint64_t bytes() const = 0;

protected:
  /// Default-onstructs an input stream.
  input_stream() = default;
};

/// An abstract output stream interface.
class output_stream
{
public:
  /// Destroys an output stream.
#ifdef VAST_CLANG
  virtual ~output_stream() = default;
#else
  virtual ~output_stream() { };
#endif

  /// Retrieves a contiguous data buffer from the stream for write operations.
  ///
  /// @param data A result parameter that contains the beginning of the next
  /// available data buffer *iff* next() returned `true`.
  ///
  /// @param size A result parameter that contains the size of *data* *iff*
  /// next() returned `true`.
  ///
  /// @return `true` if the output has a buffer available, and `false` if an
  /// error occurred.
  virtual bool next(void** data, size_t* size) = 0;

  /// Rewinds the stream position by a given number of bytes. Rewound bytes are
  /// not written into the stream. This is useful if the last buffer returned
  /// by next() is bigger than necessary.
  ///
  /// @param bytes The number of bytes to rewind the output stream.
  virtual void rewind(size_t bytes) = 0;

  /// Retrieves the number of bytes this output stream processed.
  /// @return The number of bytes this output stream processed.
  virtual uint64_t bytes() const = 0;

protected:
  /// Default-constructs an output stream.
  output_stream() = default;
};

/// An interface for buffered input.
class input_streambuffer
{
public:
  /// Attempts to read data into a given buffer.
  ///
  /// @param data The buffer receiving the result of the read operation.
  ///
  /// @param bytes The number of bytes to read.
  ///
  /// @param got The number of bytes actually read into *data*. A value of 0
  /// means that EOF has been encountered.
  ///
  /// @return `true` if *bytes* bytes could be copied into *data*  and `false`
  /// if an error occurred.
  virtual bool read(void* data, size_t bytes, size_t* got = nullptr) = 0;

  /// Skips a given number of bytes. The default implementation subsequently
  /// calls read() until the desired number of bytes have been read or an error
  /// occurred.
  ///
  /// @param bytes The number of bytes to skip.
  ///
  /// @param skipped The number of bytes actually skipped.
  ///
  /// @return `true` if *bytes* bytes were successfully skipped and false
  /// otherwise.
  virtual bool skip(size_t bytes, size_t *skipped = nullptr);
};

/// An interface for buffered output.
class output_streambuffer
{
public:
  /// Attempts to write data to a given buffer.
  ///
  /// @param data The data to write.
  ///
  /// @param bytes The number of bytes to write.
  ///
  /// @param put The number of bytes actually read from *data*.
  ///
  /// @return `true` if *bytes* bytes could be copied from *data* and false if
  /// an error occurred.
  virtual bool write(void const* data, size_t bytes, size_t* put = nullptr) = 0;
};

/// Copies data from an input stream into an output stream.
/// @param source The input stream.
/// @param sink The output stream.
/// @return The number of bytes copied for source and sink.
std::pair<size_t, size_t> copy(input_stream& source, output_stream& sink);

} // namespace io
} // namespace vast

#endif

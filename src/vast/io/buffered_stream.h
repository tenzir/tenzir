#ifndef VAST_IO_BUFFERED_STREAM_H
#define VAST_IO_BUFFERED_STREAM_H

#include <vector>
#include "vast/io/stream.h"

namespace vast {
namespace io {

/// An interface for buffered input.
class input_buffer
{
public:
  /// Attempts to read data into a given buffer.
  /// @param data The buffer receiving the result of the read operation.
  /// @param bytes The number of bytes to read.
  /// @param got The number of bytes actually read into *data*. A value of 0
  ///            means that EOF has been encountered.
  /// @returns `true` if *bytes* bytes could be copied into *data*  and `false`
  /// if an error occurred.
  virtual bool read(void* data, size_t bytes, size_t* got = nullptr) = 0;

  /// Skips a given number of bytes. The default implementation subsequently
  /// calls read() until the desired number of bytes have been read or an error
  /// occurred.
  /// @param bytes The number of bytes to skip.
  /// @param skipped The number of bytes actually skipped.
  /// @returns `true` if *bytes* bytes were successfully skipped and false
  /// otherwise.
  virtual bool skip(size_t bytes, size_t *skipped = nullptr);
};

/// An interface for buffered output.
class output_buffer
{
public:
  /// Attempts to write data to a given buffer.
  /// @param data The data to write.
  /// @param bytes The number of bytes to write.
  /// @param put The number of bytes actually read from *data*.
  /// @returns `true` if *bytes* bytes could be copied from *data* and false if
  ///          an error occurred.
  virtual bool write(void const* data, size_t bytes, size_t* put = nullptr) = 0;
};

/// An input stream that reads in a buffered fashion from a given input stream
/// buffer.
class buffered_input_stream : public input_stream
{
public:
  /// Constructs a buffered input stream from an input_buffer.
  /// @param ib The input_buffer.
  /// @param block_size The number of bytes to read at once from the underlying
  ///                   streambuffer.
  buffered_input_stream(input_buffer& ib, size_t block_size = 0);

  buffered_input_stream(buffered_input_stream&&) = default;

  buffered_input_stream& operator=(buffered_input_stream&&) = default;

  virtual bool next(void const** data, size_t* size) override;
  virtual void rewind(size_t bytes) override;
  virtual bool skip(size_t bytes) override;
  virtual uint64_t bytes() const override;

private:
  bool failed_ = false;
  int64_t position_ = 0;
  size_t rewind_bytes_ = 0;
  size_t valid_bytes_ = 0;  // Size of last call to next();
  std::vector<uint8_t> buffer_;
  input_buffer* ib_;
};

/// An output stream that buffers its data before flushing it upon destruction.
class buffered_output_stream : public output_stream
{
public:
  /// Constructs a buffered output stream from an output_buffer.
  /// @param osb The output_buffer.
  /// @param block_size The number of bytes to write at once into the
  ///                   underlying streambuffer.
  buffered_output_stream(output_buffer& osb, size_t block_size = 0);

  buffered_output_stream(buffered_output_stream&&) = default;

  virtual ~buffered_output_stream();

  buffered_output_stream& operator=(buffered_output_stream&&) = default;

  /// Flushes data to the underying output streambuffer.
  /// @returns `true` *iff* flushing succeeded.
  bool flush();

  virtual bool next(void** data, size_t* size) override;
  virtual void rewind(size_t bytes) override;
  virtual uint64_t bytes() const override;

private:
  bool failed_ = false;
  int64_t position_ = 0;
  size_t valid_bytes_ = 0;  // Size of last call to next();
  std::vector<uint8_t> buffer_;
  output_buffer* ob_;
};

} // namespace io
} // namespace vast

#endif

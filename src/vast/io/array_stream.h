#ifndef VAST_IO_ARRAY_STREAM_H
#define VAST_IO_ARRAY_STREAM_H

#include "vast/io/stream.h"

namespace vast {
namespace io {

/// An input stream that reads from a raw array.
class array_input_stream : public input_stream
{
  array_input_stream(array_input_stream const&) = delete;
  array_input_stream& operator=(array_input_stream) = delete;

public:
  /// Constructs an array input stream.
  /// @param data The beginning of the array.
  /// @param size The size of *data*.
  /// @param block_size The size in bytes used to chop up the array buffer.
  array_input_stream(void const* data, size_t size, size_t block_size = 0);

  virtual bool next(void const** data, size_t* size) override;
  virtual void rewind(size_t bytes) override;
  virtual bool skip(size_t bytes) override;
  virtual uint64_t bytes() const override;

private:
  uint8_t const* data_;
  size_t size_ = 0;
  size_t block_size_ = 0;
  size_t last_size_ = 0;
  size_t position_ = 0;
};

/// An output stream that writes into a raw array.
class array_output_stream : public output_stream
{
  array_output_stream(array_output_stream const&) = delete;
  array_output_stream& operator=(array_output_stream) = delete;

public:
  /// Constructs an array output stream.
  /// @param data The beginning of the array.
  /// @param size The size of *data*.
  /// @param block_size The size in bytes used to chop up the array buffer.
  array_output_stream(void *data, size_t size, size_t block_size = 0);

  virtual bool next(void** data, size_t* size) override;
  virtual void rewind(size_t bytes) override;
  virtual uint64_t bytes() const override;

private:
  uint8_t* data_;
  size_t size_ = 0;
  size_t block_size_ = 0;
  size_t last_size_ = 0;
  size_t position_ = 0;
};

} // namespace io
} // namespace vast

#endif

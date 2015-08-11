#ifndef VAST_IO_BUFFERED_STREAM_H
#define VAST_IO_BUFFERED_STREAM_H

#include <vector>

#include "vast/io/device.h"
#include "vast/io/stream.h"

namespace vast {
namespace io {

/// An input stream that reads in a buffered fashion from a given input stream
/// buffer.
class buffered_input_stream : public input_stream {
public:
  /// Constructs a buffered input stream from an input_device.
  /// @param idev The input_device.
  /// @param block_size The number of bytes to read at once from the underlying
  ///                   streambuffer.
  buffered_input_stream(input_device& idev, size_t block_size = 0);

  buffered_input_stream(buffered_input_stream&&) = default;

  buffered_input_stream& operator=(buffered_input_stream&&) = default;

  bool next(void const** data, size_t* size) override;
  void rewind(size_t bytes) override;
  bool skip(size_t bytes) override;
  uint64_t bytes() const override;

private:
  bool failed_ = false;
  int64_t position_ = 0;
  size_t rewind_bytes_ = 0;
  size_t valid_bytes_ = 0; // Size of last call to next();
  std::vector<uint8_t> buffer_;
  input_device* idev_;
};

/// An output stream that buffers its data before flushing it upon destruction.
class buffered_output_stream : public output_stream {
public:
  /// Constructs a buffered output stream from an output device.
  /// @param odev The output device.
  /// @param block_size The number of bytes to write at once into the
  ///                   underlying streambuffer.
  buffered_output_stream(output_device& odev, size_t block_size = 0);

  buffered_output_stream(buffered_output_stream&&) = default;

  virtual ~buffered_output_stream();

  buffered_output_stream& operator=(buffered_output_stream&&) = default;

  bool next(void** data, size_t* size) override;
  void rewind(size_t bytes) override;
  bool flush() override;
  uint64_t bytes() const override;

private:
  bool failed_ = false;
  int64_t position_ = 0;
  size_t valid_bytes_ = 0; // Size of last call to next();
  std::vector<uint8_t> buffer_;
  output_device* odev_;
};

} // namespace io
} // namespace vast

#endif

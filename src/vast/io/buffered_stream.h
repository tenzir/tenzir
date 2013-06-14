#ifndef VAST_IO_BUFFERED_STREAM_H
#define VAST_IO_BUFFERED_STREAM_H

#include <vector>
#include "vast/io/stream.h"

namespace vast {
namespace io {

/// An input stream that reads in a buffered fashion from a given input stream
/// buffer.
class buffered_input_stream : public input_stream
{
  buffered_input_stream(buffered_input_stream const&) = delete;
  buffered_input_stream& operator=(buffered_input_stream) = delete;

public:
  /// Constructs a buffered input stream from an input_streambuffer.
  ///
  /// @param isb The input_streambuffer.
  ///
  /// @param block_size The number of bytes to read at once from the underlying
  /// streambuffer.
  buffered_input_stream(input_streambuffer& isb, size_t block_size = 0);

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
  input_streambuffer& isb_;
};

/// An output stream that buffers its data before flushing it upon destruction.
class buffered_output_stream : public output_stream
{
  buffered_output_stream(buffered_output_stream const&) = delete;
  buffered_output_stream& operator=(buffered_output_stream) = delete;
public:
  /// Constructs a buffered output stream from an output_streambuffer.
  ///
  /// @param osb The output_streambuffer.
  ///
  /// @param block_size The number of bytes to write at once into the
  /// underlying streambuffer.
  buffered_output_stream(output_streambuffer& osb, size_t block_size = 0);
  virtual ~buffered_output_stream();

  /// Flushes data to the underying output streambuffer.
  /// @return `true` *iff* flushing succeeded.
  bool flush();

  virtual bool next(void** data, size_t* size) override;
  virtual void rewind(size_t bytes) override;
  virtual uint64_t bytes() const override;

private:
  bool failed_ = false;
  int64_t position_ = 0;
  size_t valid_bytes_ = 0;  // Size of last call to next();
  std::vector<uint8_t> buffer_;
  output_streambuffer& osb_;
};

} // namespace io
} // namespace vast

#endif

#ifndef VAST_IO_FILE_STREAM_H
#define VAST_IO_FILE_STREAM_H

#include "vast/filesystem.h"
#include "vast/io/buffered_stream.h"

namespace vast {
namespace io {

class file_input_device : public input_device
{
public:
  file_input_device(path const& filename);
  file_input_device(file::native_type handle, bool close_behavior);
  file_input_device(file_input_device&&) = default;
  file_input_device& operator=(file_input_device&&) = default;

  virtual bool read(void* data, size_t bytes, size_t* got) override;
  virtual bool skip(size_t bytes, size_t *skipped) override;

private:
  file file_;
};

/// An input stream that reads from a file.
class file_input_stream : public input_stream
{
public:
  /// Constructs a file input stream from a filename.
  /// @param filename The path to the file to read.
  /// @param block_size The number of bytes to read at once from the underlying
  ///                   buffer.
  explicit file_input_stream(path const& filename, size_t block_size = 0);

  /// Constructs a file input stream from a native file handle.
  /// @param handle The open file handle.
  /// @param close_behavior Whether to close the file upon destruction.
  /// @param block_size The number of bytes to read at once from the underlying
  ///                   buffer.
  file_input_stream(file::native_type handle, bool close_behavior,
                    size_t block_size = 0);

  file_input_stream(file_input_stream&& other) = default;
  file_input_stream& operator=(file_input_stream&& other) = default;

  virtual bool next(void const** data, size_t* size) override;
  virtual void rewind(size_t bytes) override;
  virtual bool skip(size_t bytes) override;
  virtual uint64_t bytes() const override;

private:
  file_input_device buffer_;
  buffered_input_stream buffered_stream_;
};

class file_output_device : public output_device
{
public:
  file_output_device(path const& filename);
  file_output_device(file::native_type handle, bool close_behavior);
  file_output_device(file_output_device&&) = default;
  file_output_device& operator=(file_output_device&&) = default;

  virtual bool write(void const* data, size_t bytes, size_t* put) override;

private:
  file file_;
};

/// An output stream that writes to a file.
class file_output_stream : public output_stream
{
public:
  /// Constructs a file output stream from a filename.
  /// @param filename The path to the file to write to.
  /// @param block_size The number of bytes to write at once to the underlying
  ///                   buffer.
  file_output_stream(path const& filename, size_t block_size = 0);

  /// Constructs a file output stream from a native file handle.
  /// @param handle The open file handle.
  /// @param close_behavior Whether to close the file upon destruction.
  /// @param block_size The number of bytes to write at once to the underlying
  ///                   buffer.
  file_output_stream(file::native_type handle, bool close_behavior,
                    size_t block_size = 0);

  virtual ~file_output_stream();

  /// Flushes data to the underying output buffer.
  /// @returns `true` *iff* flushing succeeded.
  bool flush();

  virtual bool next(void** data, size_t* size) override;
  virtual void rewind(size_t bytes) override;
  virtual uint64_t bytes() const override;

private:
  file_output_device buffer_;
  buffered_output_stream buffered_stream_;
};

} // namespace io
} // namespace vast

#endif

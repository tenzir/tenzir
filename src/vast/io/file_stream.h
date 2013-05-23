#ifndef VAST_IO_FILE_STREAM_H
#define VAST_IO_FILE_STREAM_H

#include "vast/file_system.h"
#include "vast/io/buffered_stream.h"

namespace vast {
namespace io {

/// An input stream that reads from a file. Internally, the stream employs an
/// @link input streambuffer input_streambuffer@endlink to read from the file
/// in a buffered fashion.
class file_input_stream : public input_stream
{
  file_input_stream(file_input_stream const&) = delete;
  file_input_stream& operator=(file_input_stream) = delete;

  class streambuffer : public input_streambuffer
  {
    streambuffer(streambuffer const&) = delete;
    streambuffer& operator=(streambuffer) = delete;

  public:
    streambuffer(file& f);
    virtual ~streambuffer();
    virtual bool read(void* data, size_t bytes, size_t* got) override;
    virtual bool skip(size_t bytes, size_t *skipped) override;
    void close_on_delete(bool flag);

  private:
    file& file_;
    bool close_on_delete_ = false;
  };

public:
  /// Constructs a file input stream from a file descriptor.
  ///
  /// @param file The file handle.
  ///
  /// @param block_size The number of bytes to read at once from the underlying
  /// streambuffer.
  file_input_stream(file& f, size_t block_size = 0);

  /// Controls whether to close the file when deleting this stream.
  ///
  /// @param flag If `true`, destroying this file_stream instance also closes
  /// the underlying file descriptor.
  void close_on_delete(bool flag);

  /// Closes the file.
  /// @return `true` on success.
  bool close();

  virtual bool next(void const** data, size_t* size) override;
  virtual void rewind(size_t bytes) override;
  virtual bool skip(size_t bytes) override;
  virtual uint64_t bytes() const override;

private:
  streambuffer streambuffer_;
  buffered_input_stream buffered_stream_;
};

/// An output stream that writes to a file. Internally, the stream employs an
/// @link output streambuffer output_streambuffer@endlink to write to the file
/// in a buffered fashion.
class file_output_stream : public output_stream
{
  file_output_stream(file_output_stream const&) = delete;
  file_output_stream& operator=(file_output_stream) = delete;

  class streambuffer : public output_streambuffer
  {
    streambuffer(streambuffer const&) = delete;
    streambuffer& operator=(streambuffer) = delete;

  public:
    streambuffer(file& f);
    virtual ~streambuffer();
    virtual bool write(void const* data, size_t bytes, size_t* put) override;
    void close_on_delete(bool flag);

  private:
    file& file_;
    bool close_on_delete_ = false;
  };

public:
  /// Constructs a file input stream from a file.
  ///
  /// @param f The file handle.
  ///
  /// @param block_size The number of bytes to write at once to the underlying
  /// streambuffer.
  file_output_stream(file& f, size_t block_size = 0);

  virtual ~file_output_stream();

  /// Controls whether to close the file when deleting this stream.
  ///
  /// @param flag If `true`, destroying this file_output_stream instance also
  /// closes the underlying file descriptor.
  void close_on_delete(bool flag);

  /// Flushes data to the underying output streambuffer.
  /// @return `true` *iff* flushing succeeded.
  bool flush();

  virtual bool next(void** data, size_t* size) override;
  virtual void rewind(size_t bytes) override;
  virtual uint64_t bytes() const override;

private:
  streambuffer streambuffer_;
  buffered_output_stream buffered_stream_;
};

} // namespace io
} // namespace vast

#endif

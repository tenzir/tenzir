#ifndef VAST_IO_COMPRESSED_STREAM_H
#define VAST_IO_COMPRESSED_STREAM_H

#include <memory>
#include <vector>
#include "vast/io/coded_stream.h"
#include "vast/io/compression.h"

namespace vast {
namespace io {

/// For an output stream, this value holds the default size in bytes of an
/// uncompressed data block, which is exposed to users via next(). When a
/// block fills up, it will be flushed (i.e., compressed) into the underlying
/// stream.
/// For an input stream, this value represents the buffer size of the scratch
/// space to decompress into.
static size_t const uncompressed_block_size = 64 << 10;

/// An input stream that read from a compressed input source.
class compressed_input_stream : public input_stream
{
public:
  /// Constructs a concrete compressed_input_stream.
  /// @param method The compression method.
  /// @param source The input stream to compress.
  /// @return A compressed_input_stream using *method*.
  static compressed_input_stream* create(compression method,
                                         input_stream& source);

  virtual bool next(void const** data, size_t* size) override;
  virtual void rewind(size_t bytes) override;
  virtual bool skip(size_t bytes) override;
  virtual uint64_t bytes() const override;

protected:
  /// Constructs a compressed input stream from an input stream.
  /// @param source The input stream to read from.
  compressed_input_stream(input_stream& source);

  /// Decompresses a block of data.
  /// @param source The compressed block of data.
  /// @param source_size The size of *source*.
  /// @return The number of bytes written from *source* into *sink*.
  virtual size_t uncompress(void const* source, size_t size) = 0;

  std::vector<uint8_t> compressed_;
  std::vector<uint8_t> uncompressed_;

private:
  size_t rewind_bytes_ = 0;
  size_t valid_bytes_ = 0;
  size_t total_bytes_ = 0;
  coded_input_stream source_;
};

/// An output stream that compresses data written to it.
class compressed_output_stream : public output_stream
{
public:
  /// Constructs a concrete compressed_output_stream.
  /// @param method The compression method.
  /// @param sink The output stream to compress.
  /// @return A compressed_output_stream using *method*.
  static compressed_output_stream* create(compression method,
                                          output_stream& sink);
  bool flush();

  virtual bool next(void** data, size_t* size) override;
  virtual void rewind(size_t bytes) override;
  virtual uint64_t bytes() const override;

protected:
  /// Constructs a compressed output stream from an output stream.
  ///
  /// @param sink The output stream to write to.
  ///
  /// @param block_size The size of the uncompressed scratch space which will
  /// be compressed when it fills up or when calling flush().
  compressed_output_stream(output_stream& sink, size_t block_size = 0);

  /// Retrieves a bound on the compressed size of uncompressed data.
  /// @param output The size in bytes of the data.
  /// @return The maximum number of bytes data of size *output* requires.
  virtual size_t compressed_size(size_t output) const = 0;

  /// Compresses a block of data.
  /// @param sink The sink receiving the compressed data.
  /// @param sink_size The size of *sink*.
  /// @return The number of bytes written from *source* into *sink*.
  virtual size_t compress(void* sink, size_t sink_size) = 0;

protected:
  std::vector<uint8_t> uncompressed_;
  size_t valid_bytes_ = 0;

private:
  std::vector<uint8_t> compressed_;
  size_t total_bytes_ = 0;
  coded_output_stream sink_;
};


} // namespace io
} // namespace vast

#endif

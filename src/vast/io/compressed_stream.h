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
  /// @returns The number of bytes written from *source* into *sink*.
  virtual size_t uncompress(void const* source, size_t size) = 0;

  std::vector<uint8_t> compressed_;
  std::vector<uint8_t> uncompressed_;

private:
  size_t rewind_bytes_ = 0;
  size_t valid_bytes_ = 0;
  size_t total_bytes_ = 0;
  coded_input_stream source_;
};

/// Factory function to create a ::compressed_input_stream for a given
/// compression method.
/// @param method The compression method to use.
/// @param source The underlying stream to read from.
compressed_input_stream* make_compressed_input_stream(
    compression method, input_stream& source);

/// An output stream that compresses data written to it.
class compressed_output_stream : public output_stream
{
public:
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
  /// @returns The maximum number of bytes data of size *output* requires.
  virtual size_t compressed_size(size_t output) const = 0;

  /// Compresses a block of data.
  /// @param sink The sink receiving the compressed data.
  /// @param sink_size The size of *sink*.
  /// @returns The number of bytes written from *source* into *sink*.
  virtual size_t compress(void* sink, size_t sink_size) = 0;

protected:
  std::vector<uint8_t> uncompressed_;
  size_t valid_bytes_ = 0;

private:
  std::vector<uint8_t> compressed_;
  size_t total_bytes_ = 0;
  coded_output_stream sink_;
};

/// Factory function to create a ::compressed_output_stream for a given
/// compression method.
/// @param method The compression method to use.
/// @param sink The underlying stream to write into.
compressed_output_stream* make_compressed_output_stream(
    compression method, output_stream& sink);


/// A compressed input stream that uses null compression.
class null_input_stream : public compressed_input_stream
{
public:
  null_input_stream(input_stream& source);
  virtual size_t uncompress(void const* source, size_t size) override;
};

/// A compressed output stream that uses null compression.
class null_output_stream : public compressed_output_stream
{
public:
  null_output_stream(output_stream& sink, size_t block_size = 0);

  virtual ~null_output_stream();
  virtual size_t compressed_size(size_t output) const override;
  virtual size_t compress(void* sink, size_t sink_size) override;
};


/// A compressed input stream using LZ4.
class lz4_input_stream : public compressed_input_stream
{
public:
  lz4_input_stream(input_stream& source);
  virtual size_t uncompress(void const* source, size_t size) override;
};

/// A compressed output stream using LZ4.
class lz4_output_stream : public compressed_output_stream
{
public:
  lz4_output_stream(output_stream& sink);
  virtual ~lz4_output_stream();
  virtual size_t compressed_size(size_t output) const override;
  virtual size_t compress(void* sink, size_t sink_size) override;
};

#ifdef VAST_HAVE_SNAPPY
/// A compressed input stream using Snappy.
class snappy_input_stream : public compressed_input_stream
{
public:
  snappy_input_stream(input_stream& source);
  virtual size_t uncompress(void const* source, size_t size) override;
};

/// A compressed output stream using Snappy.
class snappy_output_stream : public compressed_output_stream
{
public:
  snappy_output_stream(output_stream& sink);
  virtual ~snappy_output_stream();
  virtual size_t compressed_size(size_t output) const override;
  virtual size_t compress(void* sink, size_t sink_size) override;
};
#endif // VAST_HAVE_SNAPPY

} // namespace io
} // namespace vast

#endif

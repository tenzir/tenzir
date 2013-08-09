#include "vast/io/compressed_stream.h"

#include "vast/config.h"
#include "vast/exception.h"
#include "vast/logger.h"

#include <lz4.h>
#ifdef VAST_HAVE_SNAPPY
#include <snappy.h>
#endif // VAST_HAVE_SNAPPY

namespace vast {
namespace io {

bool compressed_input_stream::next(void const** data, size_t* size)
{
  VAST_ENTER(VAST_ARG(data, size));
  assert(! uncompressed_.empty());
  if (rewind_bytes_ > 0)
  {
    assert(rewind_bytes_ <= valid_bytes_);
    *data = uncompressed_.data() - valid_bytes_ + rewind_bytes_;
    *size = rewind_bytes_;
    rewind_bytes_ = 0;
    VAST_RETURN(true);
  }

  uint32_t compressed_block_size;
  if (! source_.read<uint32_t>(&compressed_block_size))
    VAST_RETURN(false);
  if (compressed_block_size == 0)
    throw error::io("compressed blocks may never have size 0");

  void const* src_data;
  size_t src_size;
  if (! source_.raw(&src_data, &src_size))
    VAST_RETURN(false);

  if (compressed_block_size > src_size)
  {
    // Compressed block is too big, we need to first copy from the source until
    // we have the entire block.
    compressed_.resize(compressed_block_size);
    if (! source_.read_raw(compressed_.data(), compressed_block_size))
      VAST_RETURN(false);
    valid_bytes_ = uncompress(compressed_.data(), compressed_block_size);
    if (valid_bytes_ == 0)
      VAST_RETURN(false);
  }
  else
  {
    // The full block is available as contiguous buffer from the source, we can
    // directly decompress it.
    valid_bytes_ = uncompress(src_data, compressed_block_size);
    if (! source_.skip(compressed_block_size) || valid_bytes_ == 0)
      VAST_RETURN(false);
  }

  *data = uncompressed_.data();
  *size = valid_bytes_;
  total_bytes_ += valid_bytes_;
  VAST_RETURN(true);
}

void compressed_input_stream::rewind(size_t bytes)
{
  VAST_ENTER(VAST_ARG(bytes));
  if (rewind_bytes_ + bytes <= valid_bytes_)
    rewind_bytes_ += bytes;
  else
    rewind_bytes_ = valid_bytes_;
}

bool compressed_input_stream::skip(size_t bytes)
{
  VAST_ENTER(VAST_ARG(bytes));
  void const* data;
  size_t size;
  auto ok = next(&data, &size);
  while (ok && size < bytes)
  {
    bytes -= size;
    ok = next(&data, &size);
  }
  if (size > bytes)
    rewind(size - bytes);
  VAST_RETURN(ok);
}

uint64_t compressed_input_stream::bytes() const
{
  return total_bytes_ - rewind_bytes_;
}

compressed_input_stream::compressed_input_stream(input_stream& source)
  : uncompressed_(uncompressed_block_size),
    source_(source)
{
}

compressed_input_stream* make_compressed_input_stream(
    compression method, input_stream& source)
{
  switch (method)
  {
    default:
      throw error::io("invalid compression method");
    case null:
      return new null_input_stream(source);
    case lz4:
      return new lz4_input_stream(source);
#ifdef VAST_HAVE_SNAPPY
    case snappy:
      return new snappy_input_stream(source);
#endif // VAST_HAVE_SNAPPY
  }
}


bool compressed_output_stream::flush()
{
  VAST_ENTER();
  if (valid_bytes_ == 0)
    VAST_RETURN(true);

  void* dst_data;
  size_t dst_size;
  if (! sink_.raw(&dst_data, &dst_size))
    VAST_RETURN(false);

  auto compressed_bound = compressed_size(valid_bytes_);
  compressed_.resize(compressed_bound);
  size_t n;
  if (4 + compressed_bound > dst_size)
  {
    // Block may be too large for the output stream buffer. Thus we need to
    // compress it first into a temporary buffer and then write it out in raw
    // form.
    n = compress(compressed_.data(), compressed_.size());
    assert(n > 0);
    assert(n <= std::numeric_limits<uint32_t>::max());
    assert(n <= compressed_bound);
    total_bytes_ += sink_.write<uint32_t>(&n);
    total_bytes_ += sink_.write_raw(compressed_.data(), n);
  }
  else
  {
    // We have enough space to directly write the full block into the
    // underlying output buffer, no need to use the scratch space.
    n = compress(4 + reinterpret_cast<uint8_t*>(dst_data), compressed_.size());
    assert(n > 0);
    assert(n <= std::numeric_limits<uint32_t>::max());
    assert(n <= compressed_bound);
    auto four = sink_.write<uint32_t>(&n);
    if (four != sizeof(uint32_t))
      VAST_RETURN(false);
    total_bytes_ += four + n;
    sink_.skip(n);
  }
  valid_bytes_ = 0;
  VAST_RETURN(true);
}

bool compressed_output_stream::next(void** data, size_t* size)
{
  VAST_ENTER(VAST_ARG(data, size));
  if (valid_bytes_ == uncompressed_.size() && ! flush())
    VAST_RETURN(false);

  *data = uncompressed_.data() + valid_bytes_;
  *size = uncompressed_.size() - valid_bytes_;
  valid_bytes_ = uncompressed_.size();
  VAST_RETURN(true);
}

void compressed_output_stream::rewind(size_t bytes)
{
  VAST_ENTER(VAST_ARG(bytes));
  valid_bytes_ -= bytes > valid_bytes_ ? valid_bytes_ : bytes;
}

uint64_t compressed_output_stream::bytes() const
{
  return total_bytes_;
  //return total_bytes_ + (valid_bytes_ > 0 ? compressed_size(valid_bytes_) : 0);
}

compressed_output_stream::compressed_output_stream(
    output_stream& sink, size_t block_size)
  : uncompressed_(block_size > 0 ? block_size : uncompressed_block_size),
    sink_(sink)
{
}

compressed_output_stream* make_compressed_output_stream(
    compression method, output_stream& sink)
{
  switch (method)
  {
    default:
      throw error::io("invalid compression method");
    case null:
      return new null_output_stream(sink);
    case lz4:
      return new lz4_output_stream(sink);
#ifdef VAST_HAVE_SNAPPY
    case snappy:
      return new snappy_output_stream(sink);
#endif // VAST_HAVE_SNAPPY
  }
}


null_input_stream::null_input_stream(input_stream& source)
  : compressed_input_stream(source)
{
}

size_t null_input_stream::uncompress(void const* source, size_t size)
{
  VAST_ENTER(VAST_ARG(source, size));
  assert(uncompressed_.size() >= size);
  std::memcpy(uncompressed_.data(), source, size);
  VAST_RETURN(size);
}

null_output_stream::null_output_stream(output_stream& sink, size_t block_size)
  : compressed_output_stream(sink, block_size)
{
}

null_output_stream::~null_output_stream()
{
  flush();
}

size_t null_output_stream::compressed_size(size_t output) const
{
  VAST_ENTER(VAST_ARG(output));
  VAST_RETURN(output);
}

size_t null_output_stream::compress(void* sink, size_t sink_size)
{
  VAST_ENTER(VAST_ARG(sink, sink_size));
  assert(sink_size >= valid_bytes_);
  std::memcpy(sink, uncompressed_.data(), valid_bytes_);
  VAST_RETURN(valid_bytes_);
}


lz4_input_stream::lz4_input_stream(input_stream& source)
  : compressed_input_stream(source)
{
}

size_t lz4_input_stream::uncompress(void const* source, size_t size)
{
  VAST_ENTER(VAST_ARG(source, size));
  // LZ4 does not offer functionality to estimate the output size. It operates
  // on at most 64KB blocks, so we need to ensure this maximum.
  assert(uncompressed_.size() >= 64 << 10);
  auto n = LZ4_uncompress_unknownOutputSize(
      reinterpret_cast<char const*>(source),
      reinterpret_cast<char*>(uncompressed_.data()),
      static_cast<int>(size),
      static_cast<int>(uncompressed_.size()));
  assert(n > 0);
  VAST_RETURN(n);
}

lz4_output_stream::lz4_output_stream(output_stream& sink)
  : compressed_output_stream(sink)
{
}

lz4_output_stream::~lz4_output_stream()
{
  flush();
}

size_t lz4_output_stream::compressed_size(size_t output) const
{
  VAST_ENTER(VAST_ARG(output));
  auto result = LZ4_compressBound(output);
  VAST_RETURN(result);
}

size_t lz4_output_stream::compress(void* sink, size_t sink_size)
{
  VAST_ENTER(VAST_ARG(sink, sink_size));
  assert(sink_size >= valid_bytes_);
  auto n = LZ4_compress_limitedOutput(
      reinterpret_cast<char const*>(uncompressed_.data()),
      reinterpret_cast<char*>(sink),
      static_cast<int>(valid_bytes_),
      static_cast<int>(sink_size));
  assert(n > 0);
  VAST_RETURN(n);
}


#ifdef VAST_HAVE_SNAPPY
snappy_input_stream::snappy_input_stream(input_stream& source)
  : compressed_input_stream(source)
{
}

size_t uncompress(void const* source, size_t size)
{
  VAST_ENTER(VAST_ARG(source, size));
  size_t n;
  auto success = ::snappy::GetUncompressedLength(
      reinterpret_cast<char const*>(source), size, &n);
  assert(success);
  if (uncompressed_.size() < size)
    uncompressed_.resize(64 << 10);
  success = ::snappy::RawUncompress(
      reinterpret_cast<char const*>(source),
      size,
      reinterpret_cast<char*>(uncompressed_.data()));
  assert(success);
  VAST_RETURN(n);
}

snappy_output_stream::snappy_output_stream(output_stream& sink)
  : compressed_output_stream(sink)
{
}

snappy_output_stream::~snappy_output_stream()
{
  flush();
}

size_t compressed_size(size_t output) const
{
  VAST_ENTER(VAST_ARG(output));
  auto result = ::snappy::MaxCompressedLength(output);
  VAST_RETURN(result);
}

size_t compress(void* sink, size_t sink_size)
{
  VAST_ENTER(VAST_ARG(sink, sink_size));
  size_t n;
  ::snappy::RawCompress(
      reinterpret_cast<char const*>(uncompressed_.data()),
      valid_bytes_,
      reinterpret_cast<char*>(sink),
      &n);
  assert(n <= sink_size);
  assert(n > 0);
  VAST_RETURN(n);
}
#endif // VAST_HAVE_SNAPPY

} // namespace io
} // namespace vast

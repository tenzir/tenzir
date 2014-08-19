#include "vast/block.h"

#include "vast/serialization/container.h"
#include "vast/serialization/enum.h"

namespace vast {

block::writer::writer(block& blk)
  : block_{blk},
    base_stream_{block_.buffer_},
    compressed_stream_{make_compressed_output_stream(block_.compression_,
                                                     base_stream_)},
    serializer_{*compressed_stream_}
{
}

block::writer::~writer()
{
  block_.uncompressed_bytes_ = serializer_.bytes();
}

size_t block::writer::bytes() const
{
  return serializer_.bytes();
}


block::reader::reader(block const& blk)
  : block_{blk},
    available_{block_.elements_},
    base_stream_{block_.buffer_.data(), block_.buffer_.size()},
    compressed_stream_{make_compressed_input_stream(block_.compression_,
                                                    base_stream_)},
    deserializer_{*compressed_stream_}
{
}

uint64_t block::reader::available() const
{
  return available_;
}

size_t block::reader::bytes() const
{
  return deserializer_.bytes();
}


block::block(io::compression method)
  : compression_(method)
{
}

bool block::empty() const
{
  return elements_ == 0;
}

uint64_t block::elements() const
{
  return elements_;
}

size_t block::compressed_bytes() const
{
  return buffer_.size();
}

size_t block::uncompressed_bytes() const
{
  return uncompressed_bytes_;
}

void block::serialize(serializer& sink) const
{
  sink << compression_;
  sink << elements_;
  sink << uncompressed_bytes_;
  sink << buffer_;
}

void block::deserialize(deserializer& source)
{
  source >> compression_;
  source >> elements_;
  source >> uncompressed_bytes_;
  source >> buffer_;
}

bool operator==(block const& x, block const& y)
{
  return x.compression_ == y.compression_
      && x.elements_ == y.elements_
      && x.uncompressed_bytes_ == y.uncompressed_bytes_
      && x.buffer_ == y.buffer_;
}

} // namespace vast

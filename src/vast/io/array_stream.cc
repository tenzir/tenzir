#include "vast/io/array_stream.h"

#include <cassert>
#include "vast/exception.h"

namespace vast {
namespace io {

array_input_stream::array_input_stream(
    void const* data, size_t size, size_t block_size)
  : data_(reinterpret_cast<uint8_t const*>(data))
  , size_(size)
  , block_size_(block_size > 0 ? block_size : size)
{
}

bool array_input_stream::next(void const** data, size_t* size)
{
  if (position_ == size_)
  {
    last_size_ = 0;
    return false;
  }
  assert(position_ < size_);
  last_size_ = std::min(block_size_, size_ - position_);
  *data = data_ + position_;
  *size = last_size_;
  position_ += last_size_;
  return true;
}

void array_input_stream::rewind(size_t bytes)
{
  if (last_size_ == 0)
    throw error::io("rewind can only be called after successful next");
  if (bytes > last_size_)
    throw error::io("rewind cannot rewind more bytes than available");
  position_ -= bytes;
  last_size_ = 0;
}

bool array_input_stream::skip(size_t bytes)
{
  last_size_ = 0;
  if (bytes > size_ - position_)
  {
    position_ = size_;
    return false;
  }
  position_ += bytes;
  return true;
}

uint64_t array_input_stream::bytes() const
{
  return position_;
}

array_output_stream::array_output_stream(
    void *data, size_t size, size_t block_size)
  : data_(reinterpret_cast<uint8_t*>(data))
  , size_(size)
  , block_size_(block_size > 0 ? block_size : size)
{
}


bool array_output_stream::next(void** data, size_t* size)
{
  if (position_ == size_)
  {
    last_size_ = 0;
    return false;
  }
  assert(position_ < size_);
  last_size_ = std::min(block_size_, size_ - position_);
  *data = data_ + position_;
  *size = last_size_;
  position_ += last_size_;
  return true;
}

void array_output_stream::rewind(size_t bytes)
{
  if (last_size_ == 0)
    throw error::io("rewind can only be called after successful next");
  if (bytes > last_size_)
    throw error::io("rewind cannot rewind more bytes than available");
  position_ -= bytes;
  last_size_ = 0;
}

uint64_t array_output_stream::bytes() const
{
  return position_;
}

} // namespace io
} // namespace vast

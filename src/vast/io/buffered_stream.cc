#include "vast/io/buffered_stream.h"

namespace vast {
namespace io {

buffered_input_stream::buffered_input_stream(input_device& idev,
                                             size_t block_size)
  : buffer_(block_size > 0 ? block_size : default_block_size),
    idev_{&idev}
{
}

bool buffered_input_stream::next(void const** data, size_t* size)
{
  if (failed_)
    return false;
  if (rewind_bytes_ > 0)
  {
    *data = buffer_.data() + valid_bytes_ - rewind_bytes_;
    *size = rewind_bytes_;
    rewind_bytes_ = 0;
    return true;
  }
  failed_ = ! idev_->read(buffer_.data(), buffer_.size(), &valid_bytes_);
  if (failed_ || valid_bytes_ == 0)
    return false;
  position_ += valid_bytes_;
  *size = valid_bytes_;
  *data = buffer_.data();
  return true;
}

void buffered_input_stream::rewind(size_t bytes)
{
  if (rewind_bytes_ + bytes <= valid_bytes_)
    rewind_bytes_ += bytes;
  else
    rewind_bytes_ = valid_bytes_;
}

bool buffered_input_stream::skip(size_t bytes)
{
  if (failed_)
    return false;
  if (bytes < rewind_bytes_)
  {
    rewind_bytes_ -= bytes;
    return true;
  }
  bytes -= rewind_bytes_;
  rewind_bytes_ = 0;
  auto skipped = size_t{0};
  auto success = idev_->skip(bytes, &skipped);
  if (success)
    position_ += skipped;
  return success && skipped == bytes;
}

uint64_t buffered_input_stream::bytes() const
{
  return position_ - rewind_bytes_;
}


buffered_output_stream::buffered_output_stream(
    output_device& odev, size_t block_size)
  : buffer_(block_size > 0 ? block_size : default_block_size),
    odev_{&odev}
{
}

buffered_output_stream::~buffered_output_stream()
{
  flush();
}

bool buffered_output_stream::next(void** data, size_t* size)
{
  if (valid_bytes_ == buffer_.size() && ! flush())
    return false;
  *data = buffer_.data() + valid_bytes_;
  *size = buffer_.size() - valid_bytes_;
  valid_bytes_ = buffer_.size();
  return true;
}

void buffered_output_stream::rewind(size_t bytes)
{
  valid_bytes_ -= bytes > valid_bytes_ ? valid_bytes_ : bytes;
}

bool buffered_output_stream::flush()
{
  if (failed_)
    return false;
  if (valid_bytes_ == 0)
    return true;
  if ((failed_ = ! odev_->write(buffer_.data(), valid_bytes_)))
    return false;
  position_ += valid_bytes_;
  valid_bytes_ = 0;
  return true;
}

uint64_t buffered_output_stream::bytes() const
{
  return position_ + valid_bytes_;
}

} // namespace io
} // namespace vast

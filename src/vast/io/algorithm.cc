#include "vast/io/algorithm.h"

#include <iostream>
#include <cassert>
#include <cstring>
#include <algorithm>

namespace vast {
namespace io {

input_iterator::input_iterator(input_stream& in)
  : in_{&in}
{
  buf_ = in_->next_block();
}

void input_iterator::increment()
{
  if (i_ + 1 < buf_.size())
  {
    ++i_;
  }
  else
  {
    do
    {
      buf_ = in_->next_block();
    }
    while (buf_ && buf_.size() == 0);

    i_ = 0;
  }
}

char input_iterator::dereference() const
{
  assert(i_ < buf_.size());
  return *buf_.as<char>(i_);
}

bool input_iterator::equals(input_iterator const& other) const
{
  return i_ == other.i_ && buf_ == other.buf_;
}


output_iterator::output_iterator(output_stream& out)
  : out_{&out}
{
  buf_ = out_->next_block();
}

void output_iterator::rewind()
{
  if (! buf_)
    return;

  out_->rewind(buf_.size() - i_);
}

void output_iterator::increment()
{
  if (i_ + 1 < buf_.size())
  {
    ++i_;
  }
  else
  {
    do
    {
      buf_ = out_->next_block();
    }
    while (buf_ && buf_.size() == 0);

    i_ = 0;
  }
}

char& output_iterator::dereference() const
{
  assert(i_ < buf_.size());
  return *buf_.as<char>(i_);
}


std::pair<size_t, size_t> copy(input_stream& source, output_stream& sink)
{
  auto in_bytes = source.bytes();
  auto out_bytes = sink.bytes();
  void const* in_buf;
  void* out_buf;
  size_t total = 0, in_size = 0, out_size = 0;
  while (source.next(&in_buf, &in_size))
    while (sink.next(&out_buf, &out_size))
      if (in_size <= out_size)
      {
        std::memcpy(out_buf, in_buf, in_size);
        total += in_size;
        sink.rewind(out_size - in_size);
        break;
      }
      else
      {
        std::memcpy(out_buf, in_buf, out_size);
        total += out_size;
        in_buf = reinterpret_cast<uint8_t const*>(in_buf) + out_size;
        in_size -= out_size;
      }

  return {source.bytes() - in_bytes, sink.bytes() - out_bytes};
}

} // namespace io
} // namespace vast

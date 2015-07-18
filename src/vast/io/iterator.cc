#include <cstring>

#include "vast/io/iterator.h"
#include "vast/util/assert.h"

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
  VAST_ASSERT(i_ < buf_.size());
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

size_t output_iterator::rewind()
{
  if (! buf_)
    return 0;
  auto available = buf_.size() - i_;
  if (available > 0)
  {
    out_->rewind(available);
    buf_ = {};
    i_ = 0;
  }
  return available;
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
  VAST_ASSERT(i_ < buf_.size());
  return *buf_.as<char>(i_);
}

} // namespace io
} // namespace vast

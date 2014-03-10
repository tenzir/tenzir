#include "vast/bitmap_index.h"

namespace vast {

bool bitmap_index::push_back(value const& val, event_id id)
{
  if (id > 0)
  {
    if (id < size())
    {
      VAST_LOG_ERROR("got value " << val << " incompatible ID " << id <<
                     " (required: ID > " << size() << ')');
      return false;
    }
    auto delta = id - size();
    if (delta > 1)
      if (! append(delta, false))
        return false;
  }
  return val ? push_back_impl(val) : append(1, false);
}

bool bitmap_index::empty() const
{
  return size() == 0;
}

uint64_t bitmap_index::appended() const
{
  return size() - checkpoint_size_;
}

void bitmap_index::checkpoint()
{
  checkpoint_size_ = size();
}

bool operator==(bitmap_index const& x, bitmap_index const& y)
{
  return x.equals(y);
}

} // namespace vast

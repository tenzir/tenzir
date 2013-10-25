#include "vast/bitmap_index.h"

#include "vast/container.h"
#include "vast/value.h"
#include "vast/bitmap_index/address.h"
#include "vast/bitmap_index/arithmetic.h"
#include "vast/bitmap_index/port.h"
#include "vast/bitmap_index/string.h"
#include "vast/bitmap_index/time.h"

namespace vast {

std::unique_ptr<bitmap_index> bitmap_index::create(value_type t)
{
  switch (t)
  {
    default:
      throw std::runtime_error("unsupported bitmap index type");
    case bool_type:
      return make_unique<arithmetic_bitmap_index<bool_type>>();
    case int_type:
      return make_unique<arithmetic_bitmap_index<int_type>>();
    case uint_type:
      return make_unique<arithmetic_bitmap_index<uint_type>>();
    case double_type:
      return make_unique<arithmetic_bitmap_index<double_type>>();
    case time_range_type:
    case time_point_type:
      return make_unique<time_bitmap_index>();
    case string_type:
    case regex_type:
      return make_unique<string_bitmap_index>();
    case address_type:
      return make_unique<address_bitmap_index>();
    case port_type:
      return make_unique<port_bitmap_index>();
  }
}

bool bitmap_index::push_back(value const& val, uint64_t id)
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
      if (! append(delta - 1, false))
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

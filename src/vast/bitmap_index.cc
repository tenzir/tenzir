#include "vast/bitmap_index.h"
#include "vast/bitmap_index/address.h"
#include "vast/bitmap_index/arithmetic.h"
#include "vast/bitmap_index/port.h"
#include "vast/bitmap_index/string.h"
#include "vast/bitmap_index/time.h"

#include "vast/value.h"

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

bool bitmap_index::push_back(value const& val)
{
  return (val == nil) ? append(1, false) : push_back_impl(val);
}

bool bitmap_index::empty() const
{
  return size() == 0;
}

} // namespace vast

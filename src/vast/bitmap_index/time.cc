#include "vast/bitmap_index/time.h"

#include "vast/exception.h"
#include "vast/to_string.h"
#include "vast/value.h"

namespace vast {

time_bitmap_index::time_bitmap_index(int precision)
  : bitmap_({precision}, {})
{
}

bool time_bitmap_index::patch(size_t n)
{
  return bitmap_.patch(n);
}

option<bitstream>
time_bitmap_index::lookup(relational_operator op, value const& val) const
{
  if (op == in || op == not_in)
    throw error::operation("unsupported relational operator", op);
  if (bitmap_.empty())
    return {};
  auto result = bitmap_.lookup(op, extract(val));
  if (! result)
    return {};
  return {std::move(*result)};
}

std::string time_bitmap_index::to_string() const
{
  using vast::to_string;
  return to_string(bitmap_);
}

time_range::rep time_bitmap_index::extract(value const& val)
{
  switch (val.which())
  {
    default:
      throw error::index("not a time type");
    case time_range_type:
      return val.get<time_range>().count();
    case time_point_type:
      return val.get<time_point>().since_epoch().count();
  }
}

bool time_bitmap_index::push_back_impl(value const& val)
{
  bitmap_.push_back(extract(val));
  return true;
}

void time_bitmap_index::serialize(serializer& sink) const
{
  sink << bitmap_;
}

void time_bitmap_index::deserialize(deserializer& source)
{
  source >> bitmap_;
}

} // namespace vast

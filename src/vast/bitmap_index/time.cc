#include "vast/bitmap_index/time.h"

#include "vast/value.h"

namespace vast {

time_bitmap_index::time_bitmap_index(int precision)
  : bitmap_({precision}, {})
{
}

bool time_bitmap_index::append(size_t n, bool bit)
{
  return bitmap_.append(n, bit);
}

optional<bitstream>
time_bitmap_index::lookup(relational_operator op, value const& val) const
{
  if (op == in || op == not_in)
    throw std::runtime_error(
        "unsupported relational operator " + to<std::string>(op));
  if (bitmap_.empty())
    return {};
  auto result = bitmap_.lookup(op, extract(val));
  if (! result)
    return {};
  return {std::move(*result)};
}

uint64_t time_bitmap_index::size() const
{
  return bitmap_.size();
}

bool time_bitmap_index::convert(std::string& str) const
{
  using vast::convert;
  return convert(bitmap_, str);
}

time_range::rep time_bitmap_index::extract(value const& val)
{
  switch (val.which())
  {
    default:
      throw std::runtime_error("value not a time type");
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

bool time_bitmap_index::equals(bitmap_index const& other) const
{
  if (typeid(*this) != typeid(other))
    return false;
  auto& o = static_cast<time_bitmap_index const&>(other);
  return bitmap_ == o.bitmap_;
}

void time_bitmap_index::serialize(serializer& sink) const
{
  sink << bitmap_;
}

void time_bitmap_index::deserialize(deserializer& source)
{
  source >> bitmap_;
  checkpoint();
}

} // namespace vast

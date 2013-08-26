#include "vast/bitmap_index/string.h"

#include "vast/exception.h"
#include "vast/to_string.h"
#include "vast/value.h"

namespace vast {

bool string_bitmap_index::patch(size_t n)
{
  return bitmap_.patch(n);
}

option<bitstream>
string_bitmap_index::lookup(relational_operator op, value const& val) const
{
  if (! (op == equal || op == not_equal))
    throw error::operation("unsupported relational operator", op);

  auto str = vast::to_string(val.get<string>());
  auto i = dictionary_[str];
  if (! i)
    return {};

  auto bs = bitmap_[*i];
  if (! bs)
    return {};

  return {std::move(op == equal ? *bs : bs->flip())};
}

std::string string_bitmap_index::to_string() const
{
  using vast::to_string;
  return to_string(bitmap_);
}

bool string_bitmap_index::push_back_impl(value const& val)
{
  using vast::to_string;
  auto str = to_string(val.get<string>());
  auto i = dictionary_[str];
  if (!i)
    i = dictionary_.insert(str);
  if (!i)
    return false;
  return bitmap_.push_back(*i);
}

void string_bitmap_index::serialize(serializer& sink) const
{
  sink << dictionary_ << bitmap_;
}

void string_bitmap_index::deserialize(deserializer& source)
{
  source >> dictionary_ >> bitmap_;
}

} // namespace vast

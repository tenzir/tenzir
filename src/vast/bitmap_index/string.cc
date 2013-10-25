#include "vast/bitmap_index/string.h"

#include "vast/exception.h"
#include "vast/value.h"

namespace vast {

bool string_bitmap_index::append(size_t n, bool bit)
{
  return bitmap_.append(n, bit);
}

optional<bitstream>
string_bitmap_index::lookup(relational_operator op, value const& val) const
{
  if (! (op == equal || op == not_equal))
    throw std::runtime_error("unsupported relational operator " + 
                             to<std::string>(op));

  auto i = dictionary_[to<std::string>(val.get<string>())];
  if (! i)
    return {};

  auto bs = bitmap_[*i];
  if (! bs)
    return {};

  return {std::move(op == equal ? *bs : bs->flip())};
}

uint64_t string_bitmap_index::size() const
{
  return bitmap_.size();
}

bool string_bitmap_index::push_back_impl(value const& val)
{
  auto str = to<std::string>(val.get<string>());
  auto i = dictionary_[str];
  if (!i)
    i = dictionary_.insert(str);
  if (!i)
    return false;
  return bitmap_.push_back(*i);
}

bool string_bitmap_index::equals(bitmap_index const& other) const
{
  if (typeid(*this) != typeid(other))
    return false;
  auto& o = static_cast<string_bitmap_index const&>(other);
  return bitmap_ == o.bitmap_;
}

void string_bitmap_index::serialize(serializer& sink) const
{
  sink << dictionary_ << bitmap_;
}

void string_bitmap_index::deserialize(deserializer& source)
{
  source >> dictionary_ >> bitmap_;
  checkpoint();
}

bool string_bitmap_index::convert(std::string& str) const
{
  using vast::convert;
  return convert(bitmap_, str);
}

} // namespace vast

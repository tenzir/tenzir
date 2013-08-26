#include "vast/bitmap_index/address.h"

#include "vast/exception.h"
#include "vast/value.h"

namespace vast {

bool address_bitmap_index::append(size_t n, bool bit)
{
  bool success = true;
  for (size_t i = 0; i < 16; ++i)
    if (! bitmaps_[i].append(n, bit))
      success = false;
  return v4_.append(n, bit) && success;
}

option<bitstream>
address_bitmap_index::lookup(relational_operator op, value const& val) const
{
  if (! (op == equal || op == not_equal || op == in || op == not_in))
    throw error::operation("unsupported relational operator", op);

  if (v4_.empty())
    return {};

  switch (val.which())
  {
    default:
      throw error::index("invalid value type");
    case address_type:
      return lookup(val.get<address>(), op);
    case prefix_type:
      return lookup(val.get<prefix>(), op);
  }
}

std::string address_bitmap_index::to_string() const
{
  using vast::to_string;
  std::vector<bitstream_type> v;
  v.reserve(128);
  for (size_t i = 0; i < 128; ++i)
    v.emplace_back(*bitmaps_[i / 8].lookup_raw(7 - i % 8));
  std::string str;
  for (auto& row : transpose(v))
    str += to_string(row) + '\n';
  str.pop_back();
  return str;
}

bool address_bitmap_index::push_back_impl(value const& val)
{
  auto& addr = val.get<address>();
  auto& bytes = addr.data();
  size_t const start = addr.is_v4() ? 12 : 0;
  auto success = v4_.push_back(start == 12);
  for (size_t i = 0; i < 16; ++i)
    if (! bitmaps_[i].push_back(i < start ? 0x00 : bytes[i]))
      success = false;
  return success;
}

option<bitstream>
address_bitmap_index::lookup(address const& addr, relational_operator op) const
{
  auto& bytes = addr.data();
  auto is_v4 = addr.is_v4();
  option<bitstream> result;
  result = bitstream{is_v4 ? v4_ : bitstream_type{v4_.size(), true}};
  for (size_t i = is_v4 ? 12 : 0; i < 16; ++ i)
    if (auto bs = bitmaps_[i][bytes[i]])
      *result &= *bs;
    else if (op == not_equal)
      return bitstream{bitstream_type{v4_.size(), true}};
    else
      return {};

  if (op == not_equal)
    result->flip();
  return result;
}

option<bitstream>
address_bitmap_index::lookup(prefix const& pfx, relational_operator op) const
{
  if (! (op == in || op == not_in))
    throw error::operation("unsupported relational operator", op);

  auto topk = pfx.length();
  if (topk == 0)
    throw error::index("invalid IP prefix length");

  auto net = pfx.network();
  auto is_v4 = net.is_v4();
  if ((is_v4 ? topk + 96 : topk) == 128)
    return lookup(pfx.network(), op == in ? equal : not_equal);

  option<bitstream> result;
  result = bitstream{is_v4 ? v4_ : bitstream_type{v4_.size(), true}};
  auto bit = topk;
  auto& bytes = net.data();
  for (size_t i = is_v4 ? 12 : 0; i < 16; ++ i)
    for (size_t j = 8; j --> 0; )
    {
      if (auto bs = bitmaps_[i].lookup_raw(j))
        *result &= ((bytes[i] >> j) & 1) ? *bs : ~*bs;
      else
        throw error::index("corrupt index: bit must exist");

      if (! --bit)
      {
        if (op == not_in)
          result->flip();
        return result;
      }
    }

  return {};
}

void address_bitmap_index::serialize(serializer& sink) const
{
  sink << bitmaps_ << v4_;
}

void address_bitmap_index::deserialize(deserializer& source)
{
  source >> bitmaps_ >> v4_;
}

} // namespace vast

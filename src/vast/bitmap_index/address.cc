#include "vast/bitmap_index/address.h"

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

optional<bitstream>
address_bitmap_index::lookup(relational_operator op, value const& val) const
{
  if (! (op == equal || op == not_equal || op == in || op == not_in))
    throw std::runtime_error("unsupported relational operator " + 
                             to<std::string>(op));
  if (v4_.empty())
    return {};

  switch (val.which())
  {
    default:
      throw std::runtime_error("invalid value type");
    case address_type:
      return lookup(val.get<address>(), op);
    case prefix_type:
      return lookup(val.get<prefix>(), op);
  }
}

uint64_t address_bitmap_index::size() const
{
  return v4_.size();
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

bool address_bitmap_index::equals(bitmap_index const& other) const
{
  if (typeid(*this) != typeid(other))
    return false;
  auto& o = static_cast<address_bitmap_index const&>(other);
  return bitmaps_ == o.bitmaps_ && v4_ == o.v4_;
}

optional<bitstream>
address_bitmap_index::lookup(address const& addr, relational_operator op) const
{
  auto& bytes = addr.data();
  auto is_v4 = addr.is_v4();
  optional<bitstream> result;
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

optional<bitstream>
address_bitmap_index::lookup(prefix const& pfx, relational_operator op) const
{
  if (! (op == in || op == not_in))
    throw std::runtime_error("unsupported relational operator " + 
                             to<std::string>(op));
  auto topk = pfx.length();
  if (topk == 0)
    throw std::runtime_error("invalid IP prefix length");

  auto net = pfx.network();
  auto is_v4 = net.is_v4();
  if ((is_v4 ? topk + 96 : topk) == 128)
    return lookup(pfx.network(), op == in ? equal : not_equal);

  optional<bitstream> result;
  result = bitstream{is_v4 ? v4_ : bitstream_type{v4_.size(), true}};
  auto bit = topk;
  auto& bytes = net.data();
  for (size_t i = is_v4 ? 12 : 0; i < 16; ++ i)
    for (size_t j = 8; j --> 0; )
    {
      if (auto bs = bitmaps_[i].lookup_raw(j))
        *result &= ((bytes[i] >> j) & 1) ? *bs : ~*bs;
      else
        throw std::runtime_error("corrupt index: bit must exist");

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
  checkpoint();
}

bool address_bitmap_index::convert(std::string& str) const
{
  std::vector<bitstream_type> v;
  v.reserve(128);
  for (size_t i = 0; i < 128; ++i)
    v.emplace_back(*bitmaps_[i / 8].lookup_raw(7 - i % 8));
  for (auto& row : transpose(v))
    str += to<std::string>(row) + '\n';
  str.pop_back();
  return true;
}

} // namespace vast

#include "vast/bitmap_index/port.h"

#include "vast/value.h"

namespace vast {

bool port_bitmap_index::append(size_t n, bool bit)
{
  auto success = num_.append(n, bit);
  return proto_.append(n, bit) && success;
}

option<bitstream>
port_bitmap_index::lookup(relational_operator op, value const& val) const
{
  if (op == in || op == not_in)
    throw std::runtime_error("unsupported relational operator " + 
                             to<std::string>(op));
  if (num_.empty())
    return {};
  auto& p = val.get<port>();
  auto nbs = num_.lookup(op, p.number());
  if (! nbs)
    return {};
  if (p.type() != port::unknown)
    if (auto tbs = num_[p.type()])
        *nbs &= *tbs;
  return {std::move(*nbs)};
}

uint64_t port_bitmap_index::size() const
{
  return proto_.size();
}

bool port_bitmap_index::push_back_impl(value const& val)
{
  auto& p = val.get<port>();
  num_.push_back(p.number());
  proto_.push_back(static_cast<proto_type>(p.type()));
  return true;
}

void port_bitmap_index::serialize(serializer& sink) const
{
  sink << num_ << proto_;
}

void port_bitmap_index::deserialize(deserializer& source)
{
  source >> num_ >> proto_;
}

bool port_bitmap_index::convert(std::string& str) const
{
  using vast::convert;
  return convert(num_, str);
}

} // namespace vast

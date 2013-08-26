#include "vast/bitmap_index/port.h"

#include "vast/exception.h"
#include "vast/to_string.h"
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
    throw error::operation("unsupported relational operator", op);
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

std::string port_bitmap_index::to_string() const
{
  using vast::to_string;
  return to_string(num_);
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

} // namespace vast

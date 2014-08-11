#include "vast/subnet.h"

#include "vast/logger.h"
#include "vast/serialization/arithmetic.h"
#include "vast/util/json.h"

namespace vast {

subnet::subnet()
  : length_{0u}
{
}

subnet::subnet(address addr, uint8_t length)
  : network_{std::move(addr)},
    length_{length}
{
  if (! initialize())
  {
    network_ = address{};
    length_ = 0;
  }
}

bool subnet::contains(address const& addr) const
{
  address p{addr};
  p.mask(length_);
  return p == network_;
}

address const& subnet::network() const
{
  return network_;
}

uint8_t subnet::length() const
{
  return network_.is_v4() ? length_ - 96 : length_;
}

bool subnet::initialize()
{
  if (network_.is_v4())
  {
    if (length_ > 32)
      return false;

    length_ += 96;
  }
  else if (length_ > 128)
  {
    return false;
  }

  network_.mask(length_);

  return true;
}

void subnet::serialize(serializer& sink) const
{
  VAST_ENTER(VAST_THIS);
  sink << length_ << network_;
}

void subnet::deserialize(deserializer& source)
{
  VAST_ENTER();
  source >> length_ >> network_;
  VAST_LEAVE(VAST_THIS);
}

bool operator==(subnet const& x, subnet const& y)
{
  return x.network_ == y.network_ && x.length_ == y.length_;
}

bool operator<(subnet const& x, subnet const& y)
{
  return std::tie(x.network_, x.length_) < std::tie(y.network_, y.length_);
}

trial<void> convert(subnet const& p, util::json& j)
{
  j = to_string(p);
  return nothing;
}

} // namespace vast

#include "vast/prefix.h"

#include "vast/logger.h"
#include "vast/value.h" // TODO: remove after exception removal
#include "vast/serialization.h"

namespace vast {

prefix::prefix()
  : length_{0u}
{
}

prefix::prefix(address addr, uint8_t length)
  : network_{std::move(addr)},
    length_{length}
{
  initialize();
}

prefix::prefix(prefix&& other)
  : network_{std::move(other.network_)},
    length_{other.length_}
{
  other.length_ = 0;
}

bool prefix::contains(address const& addr) const
{
  address p{addr};
  p.mask(length_);
  return p == network_;
}

address const& prefix::network() const
{
  return network_;
}

uint8_t prefix::length() const
{
  return network_.is_v4() ? length_ - 96 : length_;
}

void prefix::initialize()
{
  if (network_.is_v4())
  {
    if (length_ > 32)
      throw error::bad_value("invalid prefix", prefix_type);

    length_ += 96;
  }
  else if (length_ > 128)
  {
    throw error::bad_value("invalid prefix", prefix_type);
  }

  network_.mask(length_);
}

void prefix::serialize(serializer& sink) const
{
  VAST_ENTER(VAST_THIS);
  sink << length_;
  sink << network_;
}

void prefix::deserialize(deserializer& source)
{
  VAST_ENTER();
  source >> length_;
  source >> network_;
  VAST_LEAVE(VAST_THIS);
}

bool operator==(prefix const& x, prefix const& y)
{
  return x.network_ == y.network_ && x.length_ == y.length_;
}

bool operator<(prefix const& x, prefix const& y)
{
  return std::tie(x.network_, x.length_) < std::tie(y.network_, y.length_);
}

} // namespace vast

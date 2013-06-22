#include "vast/prefix.h"

#include "vast/logger.h"
#include "vast/value.h" // TODO: remove after exception removal
#include "vast/serialization.h"

namespace vast {

prefix::prefix()
  : length_(0u)
{
}

prefix::prefix(address addr, uint8_t length)
  : network_(std::move(addr))
  , length_(length)
{
  initialize();
}

prefix::prefix(prefix const& other)
  : network_(other.network_)
  , length_(other.length_)
{
}

prefix::prefix(prefix&& other)
  : network_(std::move(other.network_))
  , length_(other.length_)
{
  other.length_ = 0;
}

prefix& prefix::operator=(prefix other)
{
  using std::swap;
  swap(network_, other.network_);
  swap(length_, other.length_);
  return *this;
}

bool prefix::contains(address const& addr) const
{
  address p(addr);
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

void prefix::serialize(serializer& sink)
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
  return x.network() == y.network() && x.length() == y.length();
}

bool operator<(prefix const& x, prefix const& y)
{
  return std::make_tuple(x.network(), x.length()) <
    std::make_tuple(y.network(), y.length());
}

std::string to_string(prefix const& p)
{
  auto str = to_string(p.network());
  str.push_back('/');
  str += std::to_string(p.length());
  return str;
}

std::ostream& operator<<(std::ostream& out, prefix const& pfx)
{
  out << to_string(pfx);
  return out;
}

} // namespace vast

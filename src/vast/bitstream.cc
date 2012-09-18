#include "vast/bitstream.h"

namespace vast {

bitvector const& bitstream::bits() const
{
  return bits_;
}

null_bitstream& null_bitstream::operator&=(null_bitstream const& other)
{
  bits_ &= other.bits_;
  return *this;
}

null_bitstream& null_bitstream::operator|=(null_bitstream const& other)
{
  bits_ |= other.bits_;
  return *this;
}

null_bitstream& null_bitstream::operator^=(null_bitstream const& other)
{
  bits_ ^= other.bits_;
  return *this;
}

null_bitstream& null_bitstream::operator-=(null_bitstream const& other)
{
  bits_ -= other.bits_;
  return *this;
}

void null_bitstream::append(size_t n, bool bit)
{
  bits_.resize(bits_.size() + n, bit);
}

void null_bitstream::push_back(bool bit)
{
  bits_.push_back(bit);
}

void null_bitstream::flip()
{
  bits_.flip();
}

bool null_bitstream::equals(null_bitstream const& other) const
{
  return bits_ == other.bits_;
}

} // namespace vast

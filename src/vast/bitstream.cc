#include "vast/bitstream.h"

namespace vast {

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

void null_bitstream::flip()
{
  bits_.flip();
}

bool null_bitstream::equals(null_bitstream const& other) const
{
  return bits_ == other.bits_;
}

void null_bitstream::append_impl(size_t n, bool bit)
{
  bits_.resize(bits_.size() + n, bit);
}

void null_bitstream::push_back_impl(bool bit)
{
  bits_.push_back(bit);
}

} // namespace vast

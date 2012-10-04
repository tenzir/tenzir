#include "vast/bitstream.h"

namespace vast {

null_bitstream& null_bitstream::operator&=(null_bitstream const& other)
{
  if (bits_.size() < other.bits_.size())
    bits_.resize(other.bits_.size());
  bits_ &= other.bits_;
  return *this;
}

null_bitstream& null_bitstream::operator|=(null_bitstream const& other)
{
  if (bits_.size() < other.bits_.size())
    bits_.resize(other.bits_.size());
  bits_ |= other.bits_;
  return *this;
}

null_bitstream& null_bitstream::operator^=(null_bitstream const& other)
{
  if (bits_.size() < other.bits_.size())
    bits_.resize(other.bits_.size());
  bits_ ^= other.bits_;
  return *this;
}

null_bitstream& null_bitstream::operator-=(null_bitstream const& other)
{
  if (bits_.size() < other.bits_.size())
    bits_.resize(other.bits_.size());
  bits_ -= other.bits_;
  return *this;
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

void null_bitstream::clear_impl() noexcept
{
  bits_.clear();
}

null_bitstream& null_bitstream::flip()
{
  bits_.flip();
  return *this;
}

bool null_bitstream::at(size_t i) const
{
  return bits_[i];
}

size_t null_bitstream::size_impl() const
{
  return bits_.size();
}

size_t null_bitstream::empty_impl() const
{
  return bits_.empty();
}

size_t null_bitstream::find_first_impl() const
{
  return bits_.find_first();
}

size_t null_bitstream::find_next_impl(size_t i) const
{
  return bits_.find_next(i);
}

} // namespace vast

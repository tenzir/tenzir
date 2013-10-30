#include "vast/bitstream.h"

#include "vast/object.h"

namespace vast {

bitstream::bitstream(bitstream const& other)
  : concept_{other.concept_ ? other.concept_->copy() : nullptr}
{
}

bitstream::bitstream(bitstream&& other)
  : concept_{std::move(other.concept_)}
{
}

bitstream& bitstream::operator=(bitstream const& other)
{
  concept_ = other.concept_ ? other.concept_->copy() : nullptr;
  return *this;
}

bitstream& bitstream::operator=(bitstream&& other)
{
  concept_ = std::move(other.concept_);
  return *this;
}

bitstream::operator bool() const
{
  return concept_ != nullptr;
}

bool bitstream::equals(bitstream const& other) const
{
  assert(concept_);
  assert(other.concept_);
  return concept_->equals(*other.concept_);
}

void bitstream::bitwise_not()
{
  assert(concept_);
  concept_->bitwise_not();
}

void bitstream::bitwise_and(bitstream const& other)
{
  assert(concept_);
  assert(other.concept_);
  concept_->bitwise_and(*other.concept_);
}

void bitstream::bitwise_or(bitstream const& other)
{
  assert(concept_);
  assert(other.concept_);
  concept_->bitwise_or(*other.concept_);
}

void bitstream::bitwise_xor(bitstream const& other)
{
  assert(concept_);
  assert(other.concept_);
  concept_->bitwise_xor(*other.concept_);
}

void bitstream::bitwise_subtract(bitstream const& other)
{
  assert(concept_);
  assert(other.concept_);
  concept_->bitwise_subtract(*other.concept_);
}

void bitstream::append_impl(size_type n, bool bit)
{
  assert(concept_);
  concept_->append_impl(n, bit);
}

void bitstream::push_back_impl(bool bit)
{
  assert(concept_);
  concept_->push_back_impl(bit);
}

void bitstream::clear_impl() noexcept
{
  assert(concept_);
  concept_->clear_impl();
}

bool bitstream::at(size_type i) const
{
  assert(concept_);
  return concept_->at(i);
}

bitstream::size_type bitstream::size_impl() const
{
  assert(concept_);
  return concept_->size_impl();
}

bool bitstream::empty_impl() const
{
  assert(concept_);
  return concept_->empty_impl();
}

bitstream::size_type bitstream::find_first_impl() const
{
  assert(concept_);
  return concept_->find_first_impl();
}

bitstream::size_type bitstream::find_next_impl(size_type i) const
{
  assert(concept_);
  return concept_->find_next_impl(i);
}

bitstream::size_type bitstream::find_last_impl() const
{
  assert(concept_);
  return concept_->find_last_impl();
}

bitstream::size_type bitstream::find_prev_impl(size_type i) const
{
  assert(concept_);
  return concept_->find_prev_impl(i);
}

bitvector const& bitstream::bits_impl() const
{
  assert(concept_);
  return concept_->bits_impl();
}

void bitstream::serialize(serializer& sink) const
{
  if (concept_)
    sink << true << concept_;
  else
    sink << false;
}

void bitstream::deserialize(deserializer& source)
{
  bool valid;
  source >> valid;
  if (valid)
    source >> concept_;
}

bool operator==(bitstream const& x, bitstream const& y)
{
  return x.equals(y);
}

null_bitstream::null_bitstream(bitvector::size_type n, bool bit)
  : bits_(n, bit)
{
}

bool null_bitstream::equals(null_bitstream const& other) const
{
  return bits_ == other.bits_;
}

void null_bitstream::bitwise_not()
{
  bits_.flip();
}

void null_bitstream::bitwise_and(null_bitstream const& other)
{
  if (bits_.size() < other.bits_.size())
    bits_.resize(other.bits_.size());
  bits_ &= other.bits_;
}

void null_bitstream::bitwise_or(null_bitstream const& other)
{
  if (bits_.size() < other.bits_.size())
    bits_.resize(other.bits_.size());
  bits_ |= other.bits_;
}

void null_bitstream::bitwise_xor(null_bitstream const& other)
{
  if (bits_.size() < other.bits_.size())
    bits_.resize(other.bits_.size());
  bits_ ^= other.bits_;
}

void null_bitstream::bitwise_subtract(null_bitstream const& other)
{
  if (bits_.size() < other.bits_.size())
    bits_.resize(other.bits_.size());
  bits_ -= other.bits_;
}

void null_bitstream::append_impl(size_type n, bool bit)
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

bool null_bitstream::at(size_type i) const
{
  return bits_[i];
}

null_bitstream::size_type null_bitstream::size_impl() const
{
  return bits_.size();
}

bool null_bitstream::empty_impl() const
{
  return bits_.empty();
}

null_bitstream::size_type null_bitstream::find_first_impl() const
{
  return bits_.find_first();
}

null_bitstream::size_type null_bitstream::find_next_impl(size_type i) const
{
  return bits_.find_next(i);
}

null_bitstream::size_type null_bitstream::find_last_impl() const
{
  return bits_.find_last();
}

null_bitstream::size_type null_bitstream::find_prev_impl(size_type i) const
{
  return bits_.find_prev(i);
}

bitvector const& null_bitstream::bits_impl() const
{
  return bits_;
}

void null_bitstream::serialize(serializer& sink) const
{
  sink << bits_;
}

void null_bitstream::deserialize(deserializer& source)
{
  source >> bits_;
}

bool operator==(null_bitstream const& x, null_bitstream const& y)
{
  return x.bits_ == y.bits_;
}

bool operator<(null_bitstream const& x, null_bitstream const& y)
{
  return x.bits_ < y.bits_;
}

} // namespace vast

#include "vast/bitstream_polymorphic.h"
#include "vast/util/assert.h"

namespace vast {

detail::bitstream_concept::iterator::iterator(iterator const& other)
  : concept_{other.concept_ ? other.concept_->copy() : nullptr} {
}

detail::bitstream_concept::iterator::iterator(iterator&& other)
  : concept_{std::move(other.concept_)} {
}

detail::bitstream_concept::iterator& detail::bitstream_concept::iterator::
operator=(iterator const& other) {
  concept_ = other.concept_ ? other.concept_->copy() : nullptr;
  return *this;
}

detail::bitstream_concept::iterator& detail::bitstream_concept::iterator::
operator=(iterator&& other) {
  concept_ = std::move(other.concept_);
  return *this;
}

bool detail::bitstream_concept::iterator::equals(iterator const& other) const {
  VAST_ASSERT(concept_);
  VAST_ASSERT(other.concept_);
  return concept_->equals(*other.concept_);
}

void detail::bitstream_concept::iterator::increment() {
  VAST_ASSERT(concept_);
  concept_->increment();
}

detail::bitstream_concept::size_type
detail::bitstream_concept::iterator::dereference() const {
  VAST_ASSERT(concept_);
  return concept_->dereference();
}

bitstream::bitstream(bitstream const& other)
  : concept_{other.concept_ ? other.concept_->copy() : nullptr} {
}

bitstream::bitstream(bitstream&& other) : concept_{std::move(other.concept_)} {
}

bitstream& bitstream::operator=(bitstream const& other) {
  concept_ = other.concept_ ? other.concept_->copy() : nullptr;
  return *this;
}

bitstream& bitstream::operator=(bitstream&& other) {
  concept_ = std::move(other.concept_);
  return *this;
}

bitstream::operator bool() const {
  return concept_ != nullptr;
}

bool bitstream::equals(bitstream const& other) const {
  VAST_ASSERT(concept_);
  VAST_ASSERT(other.concept_);
  return concept_->equals(*other.concept_);
}

void bitstream::bitwise_not() {
  if (concept_)
    concept_->bitwise_not();
}

void bitstream::bitwise_and(bitstream const& other) {
  if (concept_ && other.concept_)
    concept_->bitwise_and(*other.concept_);
  else
    concept_.reset();
}

void bitstream::bitwise_or(bitstream const& other) {
  if (!other.concept_)
    return;

  if (concept_)
    concept_->bitwise_or(*other.concept_);
  else
    concept_ = other.concept_->copy();
}

void bitstream::bitwise_xor(bitstream const& other) {
  if (concept_ && other.concept_)
    concept_->bitwise_xor(*other.concept_);
  else
    concept_.reset();
}

void bitstream::bitwise_subtract(bitstream const& other) {
  if (concept_ && other.concept_)
    concept_->bitwise_subtract(*other.concept_);
}

void bitstream::append_impl(bitstream const& other) {
  VAST_ASSERT(concept_);
  VAST_ASSERT(other.concept_);
  concept_->append_impl(*other.concept_);
}

void bitstream::append_impl(size_type n, bool bit) {
  VAST_ASSERT(concept_);
  concept_->append_impl(n, bit);
}

void bitstream::append_block_impl(block_type block, size_type bits) {
  VAST_ASSERT(concept_);
  concept_->append_block_impl(block, bits);
}

void bitstream::push_back_impl(bool bit) {
  VAST_ASSERT(concept_);
  concept_->push_back_impl(bit);
}

void bitstream::trim_impl() {
  VAST_ASSERT(concept_);
  concept_->trim_impl();
}

void bitstream::clear_impl() noexcept {
  VAST_ASSERT(concept_);
  concept_->clear_impl();
}

bool bitstream::at(size_type i) const {
  VAST_ASSERT(concept_);
  return concept_->at(i);
}

bitstream::size_type bitstream::size_impl() const {
  VAST_ASSERT(concept_);
  return concept_->size_impl();
}

bitstream::size_type bitstream::count_impl() const {
  VAST_ASSERT(concept_);
  return concept_->count_impl();
}

bool bitstream::empty_impl() const {
  VAST_ASSERT(concept_);
  return concept_->empty_impl();
}

bitstream::const_iterator bitstream::begin_impl() const {
  VAST_ASSERT(concept_);
  return concept_->begin_impl();
}

bitstream::const_iterator bitstream::end_impl() const {
  VAST_ASSERT(concept_);
  return concept_->end_impl();
}

bool bitstream::back_impl() const {
  VAST_ASSERT(concept_);
  return concept_->back_impl();
}

bitstream::size_type bitstream::find_first_impl() const {
  VAST_ASSERT(concept_);
  return concept_->find_first_impl();
}

bitstream::size_type bitstream::find_next_impl(size_type i) const {
  VAST_ASSERT(concept_);
  return concept_->find_next_impl(i);
}

bitstream::size_type bitstream::find_last_impl() const {
  VAST_ASSERT(concept_);
  return concept_->find_last_impl();
}

bitstream::size_type bitstream::find_prev_impl(size_type i) const {
  VAST_ASSERT(concept_);
  return concept_->find_prev_impl(i);
}

bitvector const& bitstream::bits_impl() const {
  VAST_ASSERT(concept_);
  return concept_->bits_impl();
}

bool operator==(bitstream const& x, bitstream const& y) {
  return x.equals(y);
}

} // namespace vast

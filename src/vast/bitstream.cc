#include "vast/bitstream.h"

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

bitstream_iterator bitstream::begin_impl() const
{
  assert(concept_);
  return concept_->begin_impl();
}

bitstream_iterator bitstream::end_impl() const
{
  assert(concept_);
  return concept_->end_impl();
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


null_bitstream_iterator null_bitstream_iterator::begin(null_bitstream const& n)
{
  return null_bitstream_iterator{base_iterator::begin(n.bits())};
}

null_bitstream_iterator null_bitstream_iterator::end(null_bitstream const& n)
{
  return null_bitstream_iterator{base_iterator::end(n.bits())};
}

null_bitstream_iterator::null_bitstream_iterator(base_iterator const& i)
  : super{i}
{
}

auto null_bitstream_iterator::dereference() const
  -> decltype(this->base().position())
{
  return base().position();
}


null_bitstream::sequence_range::sequence_range(null_bitstream const& bs)
  : bits_{&bs.bits_}
{
  if (bits_->empty())
    next_ = bitvector::npos;
  else
    next();
}

bool null_bitstream::sequence_range::next_sequence(bitsequence& seq)
{
  if (next_ >= bits_->blocks())
    return false;

  seq.offset = next_ * bitvector::block_width;
  seq.data = bits_->block(next_);
  seq.type = seq.data == 0 || seq.data == bitvector::all_one ? fill : literal;
  seq.length = bitvector::block_width;

  while (++next_ < bits_->blocks())
    if (seq.type == fill && seq.data == bits_->block(next_))
      seq.length += bitvector::block_width;
    else
      break;

  return true;
}


null_bitstream::null_bitstream(bitvector::size_type n, bool bit)
  : bits_{n, bit}
{
}

null_bitstream::sequence_range null_bitstream::sequences() const
{
  return sequence_range{*this};
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

null_bitstream_iterator null_bitstream::begin_impl() const
{
  return null_bitstream_iterator::begin(*this);
}

null_bitstream_iterator null_bitstream::end_impl() const
{
  return null_bitstream_iterator::end(*this);
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


ewah_bitstream_iterator
ewah_bitstream_iterator::begin(ewah_bitstream const& ewah)
{
  return {ewah};
}

ewah_bitstream_iterator
ewah_bitstream_iterator::end(ewah_bitstream const& ewah)
{
  return {ewah};
}

ewah_bitstream_iterator::ewah_bitstream_iterator(ewah_bitstream const& ewah)
  : ewah_{&ewah},
    pos_{0}
{
  assert(ewah_);
  assert(ewah_->bits_.blocks() >= 2);

  scan();
}

bool ewah_bitstream_iterator::equals(ewah_bitstream_iterator const& other) const
{
  return pos_ == other.pos_;
}

void ewah_bitstream_iterator::increment()
{
  assert(ewah_);
  assert(pos_ != npos);

  if (pos_ == npos)
    return;

  // First check whether we're still processing clean 1-blocks.
  if (num_clean_ > 0)
  {
    if (bitvector::bit_index(++pos_) == 0)
      --num_clean_;
    if (num_clean_ > 0)
      return;
  }

  // Time for the dirty stuff.
  while (num_dirty_ > 0)
  {
    auto i = bitvector::bit_index(pos_);
    if (i == bitvector::block_width - 1)
    {
      // We are at last bit in a block and have to move on to the next.
      ++idx_;
      ++pos_;
      if (--num_dirty_ == 0)
        break;

      // There's at least one more dirty block coming afterwards.
      auto next = bitvector::lowest_bit(ewah_->bits_.block(idx_));
      if (next != npos)
      {
        pos_ += next;
        return;
      }

      // We will never see a dirty block made up entirely of 0s (except for
      // potentially the very last one and here we're only looking at
      // *full* dirty blocks).
      assert(! "should never happen");
    }
    else
    {
      // We're still in the middle of a dirty block.
      auto next = bitvector::next_bit(ewah_->bits_.block(idx_), i);
      if (next != npos)
      {
        pos_ += next - i;
        return;
      }
      else
      {
        // We're done with this block and set the position to end of last block
        // so that we can continue with the code above.
        pos_ += bitvector::block_width - i - 1;
        continue;
      }
    }
  }

  // Now we have another marker in front of us and have to scan it.
  scan();
}

ewah_bitstream_iterator::size_type ewah_bitstream_iterator::dereference() const
{
  assert(ewah_);
  return pos_;
}

void ewah_bitstream_iterator::scan()
{
  assert(pos_ % bitvector::block_width == 0);

  // We skip over all clean 0-blocks which don't have dirty blocks after them.
  while (idx_ < ewah_->bits_.blocks() - 2 && num_dirty_ == 0)
  {
    auto marker = ewah_->bits_.block(idx_++);
    auto zeros = ! ewah_bitstream::marker_type(marker);
    num_dirty_ = ewah_bitstream::marker_num_dirty(marker);
    auto num_clean = ewah_bitstream::marker_num_clean(marker);

    if (zeros)
    {
      pos_ += bitvector::block_width * num_clean;
    }
    else
    {
      num_clean_ += num_clean;
      break;
    }
  }

  // If we have clean 1-blocks, we don't need to do anything because we know
  // that the first 1-bit will be at the current position.
  if (num_clean_ > 0)
    return;

  // Otherwise we need to find the first 1-bit in the next block, which is
  // dirty.
  pos_ = bitvector::lowest_bit(ewah_->bits_.block(idx_));
}


ewah_bitstream::ewah_bitstream(bitvector::size_type n, bool bit)
  : bits_{n, bit}
{
}

bool ewah_bitstream::equals(ewah_bitstream const& other) const
{
  return bits_ == other.bits_;
}

void ewah_bitstream::bitwise_not()
{
  // TODO
  assert(! "not yet implemented");
  bits_.flip();
}

void ewah_bitstream::bitwise_and(ewah_bitstream const& other)
{
  // TODO
  assert(! "not yet implemented");
  if (bits_.size() < other.bits_.size())
    bits_.resize(other.bits_.size());
  bits_ &= other.bits_;
}

void ewah_bitstream::bitwise_or(ewah_bitstream const& other)
{
  // TODO
  assert(! "not yet implemented");
  if (bits_.size() < other.bits_.size())
    bits_.resize(other.bits_.size());
  bits_ |= other.bits_;
}

void ewah_bitstream::bitwise_xor(ewah_bitstream const& other)
{
  // TODO
  assert(! "not yet implemented");
  if (bits_.size() < other.bits_.size())
    bits_.resize(other.bits_.size());
  bits_ ^= other.bits_;
}

void ewah_bitstream::bitwise_subtract(ewah_bitstream const& other)
{
  // TODO
  assert(! "not yet implemented");
  if (bits_.size() < other.bits_.size())
    bits_.resize(other.bits_.size());
  bits_ -= other.bits_;
}

void ewah_bitstream::append_impl(size_type n, bool bit)
{
  assert(n > 0);

  // TODO: make the function return a boolean and indicate false.
  if (std::numeric_limits<size_type>::max() - num_bits_ < n)
    return;

  if (bits_.empty())
  {
    bits_.append(0); // Always begin with an empty marker.
  }
  else
  {
    if (num_bits_ % block_width != 0)
    {
      // Finish the current dirty block.
      auto fill = std::min(n, block_width - (num_bits_ % block_width));
      bits_.resize(bits_.size() + fill, bit);
      num_bits_ += fill;
      n -= fill;
      if (n == 0)
        return;
    }

    // We've filled the last dirty block and are now at a block boundary. At
    // that point we check if we can consolidate the last block.
    integrate_last_block();
  }

  // If whatever is left fits in a literal block, we're done.
  if (n <= block_width)
  {
    bits_.resize(bits_.size() + n, bit);
    num_bits_ += n;
    return;
  }

  auto clean_blocks = n / block_width;
  auto remaining_bits = n % block_width;

  // Invariant: the last block shall always be dirty.
  if (remaining_bits == 0)
  {
    assert(clean_blocks > 0);
    --clean_blocks;
    remaining_bits = block_width;
  }

  assert(clean_blocks > 0);
  num_bits_ += n;

  auto& marker = bits_.block(last_marker_);

  // If there are no more dirty blocks and the current marker is of the same
  // type, we reuse it.
  if (last_marker_ == bits_.blocks() - 1 && marker_type(marker) == bit)
  {
    auto marker_clean_length = marker_num_clean(marker);
    auto available = marker_clean_max - marker_clean_length;
    auto new_blocks = std::min(available, clean_blocks);

    marker = marker_num_clean(marker, marker_clean_length + new_blocks);
    clean_blocks -= new_blocks;
  }

  // Now we're ready to stuff the remaining clean words in new markers.
  if (clean_blocks > 0)
  {

    // If we add new markers and the last block is not dirty, the current
    // marker must not have a dirty count.
    if (last_marker_ == bits_.blocks() - 1)
      marker = marker_num_dirty(marker, 0);

    auto markers = clean_blocks / marker_clean_max;
    auto last = clean_blocks % marker_clean_max;

    while (markers --> 0)
      bits_.append(marker_type(marker_clean_mask, bit));

    if (last > 0)
    {
      bits_.append(marker_type(marker_num_clean(0, last), bit));
    }

    last_marker_ = bits_.blocks() - 1;
  }

  bits_.resize(bits_.size() + remaining_bits, bit);
}

void ewah_bitstream::push_back_impl(bool bit)
{
  if (bits_.empty())
    bits_.append(0); // Always begin with an empty marker.
  else if (num_bits_ % block_width == 0)
    integrate_last_block();

  bits_.push_back(bit);
  ++num_bits_;
}

void ewah_bitstream::clear_impl() noexcept
{
  bits_.clear();
  num_bits_ = last_marker_ = 0;
}

bool ewah_bitstream::at(size_type i) const
{
  // TODO
  assert(! "not yet implemented");
  return bits_[i];
}

ewah_bitstream::size_type ewah_bitstream::size_impl() const
{
  return num_bits_;
}

bool ewah_bitstream::empty_impl() const
{
  return num_bits_ == 0;
}

ewah_bitstream_iterator ewah_bitstream::begin_impl() const
{
  return ewah_bitstream_iterator::begin(*this);
}

ewah_bitstream_iterator ewah_bitstream::end_impl() const
{
  return ewah_bitstream_iterator::end(*this);
}

ewah_bitstream::size_type ewah_bitstream::find_first_impl() const
{
  // TODO
  assert(! "not yet implemented");
  return {};
}

ewah_bitstream::size_type ewah_bitstream::find_next_impl(size_type i) const
{
  // TODO
  assert(! "not yet implemented");
  return bits_.find_next(i);
}

ewah_bitstream::size_type ewah_bitstream::find_last_impl() const
{
  // TODO
  assert(! "not yet implemented");
  return bits_.find_last();
}

ewah_bitstream::size_type ewah_bitstream::find_prev_impl(size_type i) const
{
  // TODO
  assert(! "not yet implemented");
  return bits_.find_prev(i);
}

bitvector const& ewah_bitstream::bits_impl() const
{
  return bits_;
}

void ewah_bitstream::integrate_last_block()
{
  assert(num_bits_ % block_width == 0);
  assert(last_marker_ != bits_.blocks() - 1);
  auto& last_block = bits_.block(bits_.blocks() - 1);
  auto blocks_after_marker = bits_.blocks() - last_marker_ - 1;

  // Check whether we can coalesce the current dirty block with the last
  // marker. We can do so if the last block
  //
  //   (i)   is clean
  //   (ii)  directly follows a marker
  //   (iii) is *compatible* with the last marker.
  //
  // Here, compatible means that the last marker type must either match the bit
  // type of the last block or have a run length of 0 (and then change its
  // type).
  if (last_block == 0 || last_block == all_one)
  {
    // Current dirty block turns out to be clean.
    auto& marker = bits_.block(last_marker_);
    auto clean_length = marker_num_clean(marker);
    auto last_block_type = last_block != 0;
    if (blocks_after_marker == 1 && clean_length == 0)
    {
      // Adjust the type and counter of the existing marker.
      marker = marker_type(marker, last_block_type);
      marker = marker_num_clean(marker, 1);
      bits_.resize(bits_.size() - block_width);
    }
    else if (blocks_after_marker == 1 &&
             last_block_type == marker_type(marker) &&
             clean_length != marker_clean_max)
    {
      // Just update the counter of the existing marker.
      marker = marker_num_clean(marker, clean_length + 1);
      bits_.resize(bits_.size() - block_width);
    }
    else
    {
      // Replace the last block with a new marker.
      auto m = marker_num_clean(marker_type(0, last_block_type), 1);
      last_block = m;
      last_marker_ = bits_.blocks() - 1;
    }
  }
  else
  {
    // The current block is dirty.
    bump_dirty_count();
  }
}

void ewah_bitstream::bump_dirty_count()
{
  assert(num_bits_ % block_width == 0);
  auto& marker = bits_.block(last_marker_);
  auto num_dirty = marker_num_dirty(marker);
  if (num_dirty == marker_dirty_max)
  {
    // We need a new marker: replace the current dirty block with a marker and
    // append a new block.
    auto& last_block = bits_.block(bits_.blocks() - 1);
    auto dirty_block = last_block;
    last_block = marker_num_dirty(1);
    last_marker_ = bits_.blocks() - 1;
    bits_.append(dirty_block);
  }
  else
  {
    // We can still bump the counter of the current marker.
    marker = marker_num_dirty(marker, num_dirty + 1);
  }
}

void ewah_bitstream::serialize(serializer& sink) const
{
  sink << num_bits_ << last_marker_ << bits_;
}

void ewah_bitstream::deserialize(deserializer& source)
{
  source >> num_bits_ >> last_marker_ >> bits_;
}

bool operator==(ewah_bitstream const& x, ewah_bitstream const& y)
{
  return x.bits_ == y.bits_;
}

bool operator<(ewah_bitstream const& x, ewah_bitstream const& y)
{
  return x.bits_ < y.bits_;
}

} // namespace vast

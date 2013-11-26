#include "vast/bitstream.h"

namespace vast {

detail::bitstream_concept::iterator::iterator(iterator const& other)
  : concept_{other.concept_ ? other.concept_->copy() : nullptr}
{
}

detail::bitstream_concept::iterator::iterator(iterator&& other)
  : concept_{std::move(other.concept_)}
{
}

detail::bitstream_concept::iterator&
detail::bitstream_concept::iterator::operator=(
    iterator const& other)
{
  concept_ = other.concept_ ? other.concept_->copy() : nullptr;
  return *this;
}

detail::bitstream_concept::iterator&
detail::bitstream_concept::iterator::operator=(
    iterator&& other)
{
  concept_ = std::move(other.concept_);
  return *this;
}

bool detail::bitstream_concept::iterator::equals(iterator const& other) const
{
  assert(concept_);
  assert(other.concept_);
  return concept_->equals(*other.concept_);
}

void detail::bitstream_concept::iterator::increment()
{
  assert(concept_);
  concept_->increment();
}

detail::bitstream_concept::size_type
detail::bitstream_concept::iterator::dereference() const
{
  assert(concept_);
  return concept_->dereference();
}


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

void bitstream::append_block_impl(block_type block, size_type bits)
{
  assert(concept_);
  concept_->append_block_impl(block, bits);
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

bitstream::const_iterator bitstream::begin_impl() const
{
  assert(concept_);
  return concept_->begin_impl();
}

bitstream::const_iterator bitstream::end_impl() const
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


null_bitstream::iterator
null_bitstream::iterator::begin(null_bitstream const& n)
{
  return iterator{base_iterator::begin(n.bits())};
}

null_bitstream::iterator
null_bitstream::iterator::end(null_bitstream const& n)
{
  return iterator{base_iterator::end(n.bits())};
}

null_bitstream::iterator::iterator(base_iterator const& i)
  : super{i}
{
}

auto null_bitstream::iterator::dereference() const
  -> decltype(this->base().position())
{
  return base().position();
}


null_bitstream::sequence_range::sequence_range(null_bitstream const& bs)
  : bits_{&bs.bits_}
{
  if (bits_->empty())
    next_block_ = npos;
  else
    next();
}

bool null_bitstream::sequence_range::next_sequence(bitsequence& seq)
{
  if (next_block_ >= bits_->blocks())
    return false;

  seq.offset = next_block_ * block_width;
  seq.data = bits_->block(next_block_);
  seq.type = seq.data == 0 || seq.data == all_one ? fill : literal;
  seq.length = block_width;

  while (++next_block_ < bits_->blocks())
    if (seq.type == fill && seq.data == bits_->block(next_block_))
      seq.length += block_width;
    else
      break;

  return true;
}


null_bitstream::null_bitstream(size_type n, bool bit)
  : bits_{n, bit}
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

void null_bitstream::append_block_impl(block_type block, size_type bits)
{
  bits_.append(block, bits);
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

null_bitstream::const_iterator null_bitstream::begin_impl() const
{
  return const_iterator::begin(*this);
}

null_bitstream::const_iterator null_bitstream::end_impl() const
{
  return const_iterator::end(*this);
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


ewah_bitstream::iterator
ewah_bitstream::iterator::begin(ewah_bitstream const& ewah)
{
  return {ewah};
}

ewah_bitstream::iterator
ewah_bitstream::iterator::end(ewah_bitstream const& /* ewah */)
{
  return {};
}

ewah_bitstream::iterator::iterator(ewah_bitstream const& ewah)
  : ewah_{&ewah},
    pos_{0}
{
  assert(ewah_);
  if (ewah_->bits_.blocks() >= 2)
    scan();
  else
    pos_ = npos;
}

bool ewah_bitstream::iterator::equals(iterator const& other) const
{
  return pos_ == other.pos_;
}

void ewah_bitstream::iterator::increment()
{
  assert(ewah_);
  assert(pos_ != npos);

  if (pos_ == npos)
    return;

  // First check whether we're processing the last (dirty) block.
  // special one.
  if (idx_ == ewah_->bits_.blocks() - 1)
  {
    auto i = bitvector::bit_index(pos_);
    auto next = bitvector::next_bit(ewah_->bits_.block(idx_), i);
    pos_ += next == npos ? npos - pos_ : next - i;
    return;
  }

  // Check whether we're still processing clean 1-blocks.
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
        pos_ += block_width - i - 1;
        continue;
      }
    }
  }

  // Now we have another marker in front of us and have to scan it.
  scan();
}

ewah_bitstream::size_type ewah_bitstream::iterator::dereference() const
{
  assert(ewah_);
  return pos_;
}

void ewah_bitstream::iterator::scan()
{
  assert(pos_ % block_width == 0);

  // We skip over all clean 0-blocks which don't have dirty blocks after them.
  while (idx_ < ewah_->bits_.blocks() - 1 && num_dirty_ == 0)
  {
    auto marker = ewah_->bits_.block(idx_++);
    auto zeros = ! ewah_bitstream::marker_type(marker);
    num_dirty_ = ewah_bitstream::marker_num_dirty(marker);
    auto num_clean = ewah_bitstream::marker_num_clean(marker);

    if (zeros)
    {
      pos_ += block_width * num_clean;
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
  // dirty. However, this dirty block may be the last block and if it doesn't
  // have a single 1-bit we're done.
  auto block = ewah_->bits_.block(idx_);
  if (idx_ == ewah_->bits_.blocks() - 1 && ! block)
  {
    pos_ = npos;
  }
  else
  {
    assert(block);
    pos_ += bitvector::lowest_bit(block);
  }
}


ewah_bitstream::sequence_range::sequence_range(ewah_bitstream const& bs)
  : bits_{&bs.bits_}
{
  if (bits_->empty())
    next_block_ = npos;
  else
    next();
}

bool ewah_bitstream::sequence_range::next_sequence(bitsequence& seq)
{
  if (next_block_ >= bits_->blocks())
    return false;

  auto block = bits_->block(next_block_++);
  if (num_dirty_ > 0 || next_block_ == bits_->blocks())
  {
    // The next block must be a dirty block (unless it's the last block, which
    // we don't count in the number of dirty blocks).
    --num_dirty_;
    seq.type = literal;
    seq.data = block;
    seq.offset += seq.length;
    seq.length = next_block_ == bits_->blocks()
      ? bitvector::bit_index(bits_->size() - 1) + 1
      : block_width;
  }
  else
  {
    // The next block is a marker.
    auto clean = marker_num_clean(block);
    num_dirty_ = marker_num_dirty(block);
    if (clean == 0)
    {
      // If the marker has no clean blocks, we can't record a fill sequence and
      // have to go to the next (literal) block.
      return next();
    }
    else
    {
      seq.type = fill;
      seq.data = marker_type(block) ? all_one : 0;
      seq.offset += seq.length;
      seq.length = clean * block_width;

      // If no dirty blocks follow this marker and we have not reached the
      // final dirty block yet, we know that the next block must be a marker as
      // well and check whether we can merge it into the current sequence.
      while (num_dirty_ == 0 && next_block_ + 1 < bits_->blocks())
      {
        auto next_marker = bits_->block(next_block_);
        auto next_type = marker_type(next_marker);
        if ((next_type && ! seq.data) || (! next_type && seq.data))
          break;

        seq.length += marker_num_clean(next_marker) * block_width;
        num_dirty_ = marker_num_dirty(next_marker);
        ++next_block_;
      }
    }
  }

  return true;
}


ewah_bitstream::ewah_bitstream(size_type n, bool bit)
{
  append(n, bit);
}

bool ewah_bitstream::equals(ewah_bitstream const& other) const
{
  return bits_ == other.bits_;
}

void ewah_bitstream::bitwise_not()
{
  if (bits_.empty())
    return;

  assert(bits_.blocks() >= 2);
  size_type next_marker = 0;
  size_type i;
  for (i = 0; i < bits_.blocks() - 1; ++i)
  {
    auto& block = bits_.block(i);
    if (i == next_marker)
    {
      next_marker += marker_num_dirty(block) + 1;
      if (marker_num_clean(block) > 0)
        block ^= msb_one;
    }
    else
    {
      block = ~block;
    }
  }

  // We only flip the active bits in the last block.
  auto idx = bitvector::bit_index(bits_.size() - 1);
  bits_.block(i) ^= all_one >> (block_width - idx - 1);
}

void ewah_bitstream::bitwise_and(ewah_bitstream const& other)
{
  *this = and_(*this, other);
}

void ewah_bitstream::bitwise_or(ewah_bitstream const& other)
{
  *this = or_(*this, other);
}

void ewah_bitstream::bitwise_xor(ewah_bitstream const& other)
{
  *this = xor_(*this, other);
}

void ewah_bitstream::bitwise_subtract(ewah_bitstream const& other)
{
  *this = nand_(*this, other);
}

void ewah_bitstream::append_impl(size_type n, bool bit)
{
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

  // If we have currently no dirty blocks and the current marker is of the same
  // type, we reuse it. We also reuse the very first marker if it's still
  // empty.
  if ((last_marker_ == bits_.blocks() - 1 && marker_type(marker) == bit)
      || (last_marker_ == 0 && marker == 0))
  {
    auto marker_clean_length = marker_num_clean(marker);
    auto available = marker_clean_max - marker_clean_length;
    auto new_blocks = std::min(available, clean_blocks);

    marker = marker_num_clean(marker, marker_clean_length + new_blocks);
    marker = marker_type(marker, bit);
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

void ewah_bitstream::append_block_impl(block_type block, size_type bits)
{
  if (bits_.empty())
    bits_.append(0); // Always begin with an empty marker.
  else if (num_bits_ % block_width == 0)
    integrate_last_block();

  if (num_bits_ % block_width == 0)
  {
    bits_.append(block, bits);
    num_bits_ += bits;
  }
  else
  {
    auto used = bits_.extra_bits();
    auto unused = block_width - used;
    if (bits <= unused)
    {
      bits_.append(block, bits);
      num_bits_ += bits;
    }
    else
    {
      bits_.append(block, unused);
      num_bits_ += unused;
      integrate_last_block();
      auto remaining = bits - unused;
      bits_.append(block >> unused, remaining);
      num_bits_ += remaining;
    }
  }
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
  for (auto& seq : sequence_range{*this})
    if (i >= seq.offset && i < seq.offset + seq.length)
      return seq.is_fill() ? seq.data : seq.data & bitvector::bit_mask(i);

  auto msg = "EWAH element out-of-range element access at index ";
  throw std::out_of_range{msg + std::to_string(i)};
}

ewah_bitstream::size_type ewah_bitstream::size_impl() const
{
  return num_bits_;
}

bool ewah_bitstream::empty_impl() const
{
  return num_bits_ == 0;
}

ewah_bitstream::const_iterator ewah_bitstream::begin_impl() const
{
  return const_iterator::begin(*this);
}

ewah_bitstream::const_iterator ewah_bitstream::end_impl() const
{
  return const_iterator::end(*this);
}

ewah_bitstream::size_type ewah_bitstream::find_first_impl() const
{
  return find_forward(0);
}

ewah_bitstream::size_type ewah_bitstream::find_next_impl(size_type i) const
{
  return i == npos || i + 1 == npos ? npos : find_forward(i + 1);
}

ewah_bitstream::size_type ewah_bitstream::find_last_impl() const
{
  return find_backward(npos);
}

ewah_bitstream::size_type ewah_bitstream::find_prev_impl(size_type i) const
{
  return i == 0 ? npos : find_backward(i - 1);
}

bitvector const& ewah_bitstream::bits_impl() const
{
  return bits_;
}

void ewah_bitstream::integrate_last_block()
{
  assert(num_bits_ % block_width == 0);
  assert(last_marker_ != bits_.blocks() - 1);
  auto& last_block = bits_.last_block();
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
    auto& last_block = bits_.last_block();
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

ewah_bitstream::size_type ewah_bitstream::find_forward(size_type i) const
{
  auto range = sequence_range{*this};

  for (auto& seq : range)
    if (seq.offset + seq.length > i)
      break;

  for (auto& seq : range)
  {
    if (seq.data)
    {
      if (seq.is_fill())
        return i >= seq.offset && i < seq.offset + seq.length ? i : seq.offset;

      auto const idx = bitvector::bit_index(i);
      if (idx == 0)
        return seq.offset + bitvector::lowest_bit(seq.data);

      auto next = bitvector::next_bit(seq.data, idx - 1);
      if (next != npos)
        return seq.offset + next;
    }
  }

  return npos;
}

ewah_bitstream::size_type ewah_bitstream::find_backward(size_type i) const
{
  size_type last = npos;
  auto range = sequence_range{*this};

  for (auto& seq : range)
  {
    if (seq.offset + seq.length > i)
    {
      if (! seq.data)
        return last;

      if (seq.is_fill())
        return seq.offset + seq.length - 1;

      auto const idx = bitvector::bit_index(i);
      if (idx == bitvector::block_width - 1)
        return seq.offset + bitvector::highest_bit(seq.data);

      auto const prev = bitvector::prev_bit(seq.data, idx + 1);
      return prev == npos ? last : seq.offset + prev;
    }

    if (seq.data)
      last = seq.offset +
        (seq.is_fill() ? seq.length - 1 : bitvector::highest_bit(seq.data));
  }

  return last;
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

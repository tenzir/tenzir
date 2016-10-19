#include "vast/ewah_bitmap.hpp"

namespace vast {

namespace {

template <class Block>
struct ewah_algorithm {
  using word = word<Block>;

  // The offset from the LSB which separates clean and dirty counters.
  static constexpr auto clean_dirty_divide = word::width / 2 - 1;

  // The mask to apply to a marker word to extract the counter of dirty words.
  static constexpr auto marker_dirty_mask = ~(word::all << clean_dirty_divide);

  // The maximum value of the counter of dirty words.
  static constexpr auto marker_dirty_max = marker_dirty_mask;

  // The mask to apply to a marker word to extract the counter of clean words.
  static constexpr auto marker_clean_mask = ~(marker_dirty_mask | word::msb1);

  /// The maximum value of the counter of clean words.
  static constexpr auto marker_clean_max
    = marker_clean_mask >> clean_dirty_divide;

  // Retrieves the type of the clean word in a marker word.
  static constexpr bool marker_type(Block block) {
    return (block & word::msb1) == word::msb1;
  }

  // Sets the marker type.
  static constexpr Block marker_type(Block block, bool type) {
    return (block & ~word::msb1) | (type ? word::msb1 : 0);
  }

  // Retrieves the number of clean words in a marker word.
  static constexpr Block marker_num_clean(Block block) {
    return (block & marker_clean_mask) >> clean_dirty_divide;
  }

  // Sets the number of clean words in a marker word.
  static constexpr Block marker_num_clean(Block block, Block n) {
    return (block & ~marker_clean_mask) | (n << clean_dirty_divide);
  }

  // Retrieves the number of dirty words following a marker word.
  static constexpr Block marker_num_dirty(Block block) {
    return block & marker_dirty_mask;
  }

  // Sets the number of dirty words in a marker word.
  static constexpr Block marker_num_dirty(Block block, Block n) {
    return (block & ~marker_dirty_mask) | n;
  }
};

using ewah = ewah_algorithm<ewah_bitmap::block_type>;

} // namespace <anonymous>

ewah_bitmap::ewah_bitmap(size_type n, bool bit) {
  append_bits(bit, n);
}

bool ewah_bitmap::empty() const {
  return num_bits_ == 0;
}

ewah_bitmap::size_type ewah_bitmap::size() const {
  return num_bits_;
}

ewah_bitmap::block_vector const& ewah_bitmap::blocks() const {
  return blocks_;
}

void ewah_bitmap::append_bit(bool bit) {
  auto partial = num_bits_ % ewah::word::width;
  if (blocks_.empty()) {
    blocks_.push_back(0); // Always begin with an empty marker.
    blocks_.push_back(ewah::word::none);
  } else if (partial == 0) {
    integrate_last_block();
    blocks_.push_back(ewah::word::none);
  }
  if (bit)
    blocks_.back() |= ewah::word::lsb1 << partial;
  ++num_bits_;
  return;
}

void ewah_bitmap::append_bits(bool bit, size_type n) {
  if (n == 0)
    return;
  if (blocks_.empty()) {
    blocks_.push_back(0); // Always begin with an empty marker.
  } else {
    auto partial = num_bits_ % ewah::word::width;
    if (partial > 0) {
      // Finish the current dirty block.
      auto fill = std::min(n, ewah::word::width - partial);
      if (bit)
        blocks_.back() |= ewah::word::lsb_mask(fill) << partial;
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
  if (n <= ewah::word::width) {
    blocks_.push_back(bit ? ewah::word::lsb_fill(n) : ewah::word::none);
    num_bits_ += n;
    return;
  }
  // At this point, we have enough bits remaining to generate clean blocks.
  VAST_ASSERT(n >= ewah::word::width);
  auto clean_blocks = n / ewah::word::width;
  auto remaining_bits = n % ewah::word::width;
  // Invariant: the last block shall always be dirty.
  if (remaining_bits == 0) {
    VAST_ASSERT(clean_blocks > 0);
    --clean_blocks;
    remaining_bits = ewah::word::width;
  }
  VAST_ASSERT(clean_blocks > 0);
  num_bits_ += n;
  auto& marker = blocks_[last_marker_];
  // If we have currently no dirty blocks and the current marker is of the same
  // type, we reuse it. We also reuse the very first marker if it's still
  // empty.
  if ((last_marker_ == blocks_.size() - 1 && ewah::marker_type(marker) == bit)
      || (last_marker_ == 0 && marker == 0)) {
    auto marker_clean_length = ewah::marker_num_clean(marker);
    auto available = ewah::marker_clean_max - marker_clean_length;
    auto new_blocks = std::min(available, clean_blocks);
    marker = ewah::marker_num_clean(marker, marker_clean_length + new_blocks);
    marker = ewah::marker_type(marker, bit);
    clean_blocks -= new_blocks;
  }
  // Now we're ready to stuff the remaining clean words in new markers.
  if (clean_blocks > 0) {
    // If we add new markers and the last block is not dirty, the current
    // marker must not have a dirty count.
    if (last_marker_ == blocks_.size() - 1)
      marker = ewah::marker_num_dirty(marker, 0);
    auto markers = clean_blocks / ewah::marker_clean_max;
    auto last = clean_blocks % ewah::marker_clean_max;
    while (markers --> 0)
      blocks_.push_back(ewah::marker_type(ewah::marker_clean_mask, bit));
    if (last > 0)
      blocks_.push_back(
        ewah::marker_type(ewah::marker_num_clean(0, last), bit));
    last_marker_ = blocks_.size() - 1;
  }
  // Add remaining stray bits.
  if (remaining_bits > 0) {
    auto block = bit ? ewah::word::lsb_fill(remaining_bits) : ewah::word::none;
    blocks_.push_back(block);
  }
}

void ewah_bitmap::append_block(block_type value, size_type bits) {
  VAST_ASSERT(bits > 0);
  VAST_ASSERT(bits <= ewah::word::width);
  if (blocks_.empty())
    blocks_.push_back(0); // Always begin with an empty marker.
  else if (num_bits_ % ewah::word::width == 0)
    integrate_last_block();
  auto partial = num_bits_ % ewah::word::width;
  if (partial == 0) {
    blocks_.push_back(value & ewah::word::lsb_fill(bits));
    num_bits_ += bits;
  } else {
    auto unused = ewah::word::width - partial;
    if (bits <= unused) {
      blocks_.back() |= (value & ewah::word::lsb_fill(bits)) << partial;
      num_bits_ += bits;
    } else {
      // Finish last
      blocks_.back() |= (value & ewah::word::lsb_fill(unused)) << partial;
      num_bits_ += unused;
      integrate_last_block();
      auto remaining = bits - unused;
      blocks_.push_back((value >> unused) & ewah::word::lsb_fill(remaining));
      num_bits_ += remaining;
    }
  }
}

void ewah_bitmap::flip() {
  if (blocks_.empty())
    return;
  VAST_ASSERT(blocks_.size() >= 2);
  auto next_marker = size_type{0};
  for (auto i = 0u; i < blocks_.size() - 1; ++i) {
    auto& block = blocks_[i];
    if (i == next_marker) {
      if (ewah::marker_num_clean(block) > 0)
        block ^= ewah::word::msb1;
      next_marker += ewah::marker_num_dirty(block) + 1;
    } else {
      block = ~block;
    }
  }
  // Only flip the active bits in the last block.
  auto partial = num_bits_ % ewah::word::width;
  blocks_.back() ^= ewah::word::lsb_mask(partial);
}

void ewah_bitmap::integrate_last_block() {
  VAST_ASSERT(num_bits_ % ewah::word::width == 0);
  VAST_ASSERT(last_marker_ != blocks_.size() - 1);
  auto& last_block = blocks_.back();
  auto blocks_after_marker = blocks_.size() - last_marker_ - 1;
  // Check whether we can coalesce the current dirty block with the last
  // marker. We can do so if the last block
  //     (1) is clean
  //     (2) directly follows a marker
  //     (3) is compatible with the last marker.
  // Here, compatible means that the last marker type must either match the bit
  // type of the last block or have a run length of 0 (and then change its
  // type).
  if (ewah::word::all_or_none(last_block)) {
    // Current dirty block turns out to be clean. (1)
    auto& marker = blocks_[last_marker_];
    auto clean_length = ewah::marker_num_clean(marker);
    auto last_block_type = !!last_block;
    if (blocks_after_marker == 1 && clean_length == 0) {
      // Adjust the type and counter of the existing marker.
      marker = ewah::marker_type(marker, last_block_type);
      marker = ewah::marker_num_clean(marker, 1);
      blocks_.pop_back();
    } else if (blocks_after_marker == 1
               && last_block_type == ewah::marker_type(marker)
               && clean_length != ewah::marker_clean_max) {
      // Just update the counter of the existing marker.
      marker = ewah::marker_num_clean(marker, clean_length + 1);
      blocks_.pop_back();
    } else {
      // Replace the last block with a new marker.
      auto m = ewah::marker_num_clean(ewah::marker_type(0, last_block_type), 1);
      last_block = m;
      last_marker_ = blocks_.size() - 1;
    }
  } else {
    // The current block is dirty.
    bump_dirty_count();
  }
}

void ewah_bitmap::bump_dirty_count() {
  VAST_ASSERT(num_bits_ % ewah::word::width == 0);
  auto& marker = blocks_[last_marker_];
  auto num_dirty = ewah::marker_num_dirty(marker);
  if (num_dirty == ewah::marker_dirty_max) {
    // We need a new marker: replace the current dirty block with a marker and
    // append a new block.
    auto& last_block = blocks_.back();
    auto dirty_block = last_block;
    last_block = ewah::marker_num_dirty(1);
    last_marker_ = blocks_.size() - 1;
    blocks_.push_back(dirty_block);
  } else {
    // We can still bump the counter of the current marker.
    marker = ewah::marker_num_dirty(marker, num_dirty + 1);
  }
}

bool operator==(ewah_bitmap const& x, ewah_bitmap const& y) {
  // If the block vector and the number of bits are equal, so must be the
  // marker by construction.
  return x.blocks_ == y.blocks_ && x.num_bits_ == y.num_bits_;
}

ewah_bitmap_range::ewah_bitmap_range(ewah_bitmap const& bm)
  : bm_{&bm} {
  if (!bm_->empty())
    scan();
}

bool ewah_bitmap_range::done() const {
  return next_ == bm_->blocks().size();
}

void ewah_bitmap_range::next() {
  if (++next_ != bm_->blocks().size())
    scan();
}

void ewah_bitmap_range::scan() {
  VAST_ASSERT(next_ < bm_->blocks().size());
  auto block = bm_->blocks()[next_];
  if (next_ + 1 == bm_->blocks().size()) {
    // The ast block; always dirty.
    auto partial = bm_->size() % ewah::word::width;
    bits_ = {block, partial == 0 ? ewah::word::width : partial};
  } else if (num_dirty_ > 0) {
    // An intermediate dirty block.
    --num_dirty_;
    bits_ = {block, ewah::word::width};
  } else {
    // A marker.
    auto num_clean = ewah::marker_num_clean(block);
    num_dirty_ = ewah::marker_num_dirty(block);
    if (num_clean == 0) {
      // If the marker has no clean blocks, we can't record a fill sequence and
      // have to go to the next (literal) block.
      ++next_;
      scan();
    } else {
      auto data = ewah::marker_type(block) ? ewah::word::all : ewah::word::none;
      auto length = num_clean * ewah::word::width;
      // If no dirty blocks follow this marker and we have not reached the
      // final dirty block yet, we know that the next block must be a marker as
      // well and check whether we can incorporate it into this sequence.
      while (num_dirty_ == 0 && next_ + 2 < bm_->blocks().size()) {
        auto next_marker = bm_->blocks()[next_ + 1];
        auto next_type = ewah::marker_type(next_marker);
        if ((next_type && !data) || (!next_type && data))
          break; // not compatible with current run
        length += ewah::marker_num_clean(next_marker) * ewah::word::width;
        num_dirty_ = ewah::marker_num_dirty(next_marker);
        ++next_;
      }
      bits_ = {data, length};
    }
  }
}

ewah_bitmap_range bit_range(ewah_bitmap const& bm) {
  return ewah_bitmap_range{bm};
}

} // namespace vast

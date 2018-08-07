/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#include "vast/ewah_bitmap.hpp"

namespace vast {

ewah_bitmap::ewah_bitmap(size_type n, bool bit) {
  append_bits(bit, n);
}

bool ewah_bitmap::empty() const {
  return num_bits_ == 0;
}

ewah_bitmap::size_type ewah_bitmap::size() const {
  return num_bits_;
}

const ewah_bitmap::block_vector& ewah_bitmap::blocks() const {
  return blocks_;
}

void ewah_bitmap::append_bit(bool bit) {
  auto partial = num_bits_ % word_type::width;
  if (blocks_.empty()) {
    blocks_.push_back(0); // Always begin with an empty marker.
    blocks_.push_back(word_type::none);
  } else if (partial == 0) {
    integrate_last_block();
    blocks_.push_back(word_type::none);
  }
  if (bit)
    blocks_.back() |= word_type::lsb1 << partial;
  ++num_bits_;
  return;
}

void ewah_bitmap::append_bits(bool bit, size_type n) {
  if (n == 0)
    return;
  if (blocks_.empty()) {
    blocks_.push_back(0); // Always begin with an empty marker.
  } else {
    auto partial = num_bits_ % word_type::width;
    if (partial > 0) {
      // Finish the current dirty block.
      auto fill = std::min(n, word_type::width - partial);
      if (bit)
        blocks_.back() |= word_type::lsb_mask(fill) << partial;
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
  if (n <= word_type::width) {
    blocks_.push_back(bit ? word_type::lsb_fill(n) : word_type::none);
    num_bits_ += n;
    return;
  }
  // At this point, we have enough bits remaining to generate clean blocks.
  VAST_ASSERT(n >= word_type::width);
  auto clean_blocks = n / word_type::width;
  auto remaining_bits = n % word_type::width;
  // Invariant: the last block shall always be dirty.
  if (remaining_bits == 0) {
    VAST_ASSERT(clean_blocks > 0);
    --clean_blocks;
    remaining_bits = word_type::width;
  }
  VAST_ASSERT(clean_blocks > 0);
  num_bits_ += n;
  auto& marker = blocks_[last_marker_];
  // If we have currently no dirty blocks and the current marker is of the same
  // type, we reuse it. We also reuse the very first marker if it's still
  // empty.
  if ((last_marker_ == blocks_.size() - 1 && word_type::marker_type(marker) == bit)
      || (last_marker_ == 0 && marker == 0)) {
    auto marker_clean_length = word_type::marker_num_clean(marker);
    auto available = word_type::marker_clean_max - marker_clean_length;
    auto new_blocks = std::min(available, clean_blocks);
    marker = word_type::marker_num_clean(marker, marker_clean_length + new_blocks);
    marker = word_type::marker_type(marker, bit);
    clean_blocks -= new_blocks;
  }
  // Now we're ready to stuff the remaining clean words in new markers.
  if (clean_blocks > 0) {
    // If we add new markers and the last block is not dirty, the current
    // marker must not have a dirty count.
    if (last_marker_ == blocks_.size() - 1)
      marker = word_type::marker_num_dirty(marker, 0);
    auto markers = clean_blocks / word_type::marker_clean_max;
    auto last = clean_blocks % word_type::marker_clean_max;
    blocks_.resize(blocks_.size() + markers,
                   word_type::marker_type(word_type::marker_clean_mask, bit));
    if (last > 0)
      blocks_.push_back(
        word_type::marker_type(word_type::marker_num_clean(0, last), bit));
    last_marker_ = blocks_.size() - 1;
  }
  // Add remaining stray bits.
  if (remaining_bits > 0) {
    auto block = bit ? word_type::lsb_fill(remaining_bits) : word_type::none;
    blocks_.push_back(block);
  }
}

void ewah_bitmap::append_block(block_type value, size_type bits) {
  VAST_ASSERT(bits > 0);
  VAST_ASSERT(bits <= word_type::width);
  if (blocks_.empty())
    blocks_.push_back(0); // Always begin with an empty marker.
  else if (num_bits_ % word_type::width == 0)
    integrate_last_block();
  auto partial = num_bits_ % word_type::width;
  if (partial == 0) {
    blocks_.push_back(value & word_type::lsb_fill(bits));
    num_bits_ += bits;
  } else {
    auto unused = word_type::width - partial;
    if (bits <= unused) {
      blocks_.back() |= (value & word_type::lsb_fill(bits)) << partial;
      num_bits_ += bits;
    } else {
      // Finish last
      blocks_.back() |= (value & word_type::lsb_fill(unused)) << partial;
      num_bits_ += unused;
      integrate_last_block();
      auto remaining = bits - unused;
      blocks_.push_back((value >> unused) & word_type::lsb_fill(remaining));
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
      if (word_type::marker_num_clean(block) > 0)
        block ^= word_type::msb1;
      next_marker += word_type::marker_num_dirty(block) + 1;
    } else {
      block = ~block;
    }
  }
  // Flip the last (dirty) block manually, because next_marker would always
  // point to it.
  blocks_.back() = ~blocks_.back();
  // Make sure we didn't flip unused bits in the last block.
  auto partial = num_bits_ % word_type::width;
  if (partial > 0)
    blocks_.back() &= word_type::lsb_mask(partial);
}

void ewah_bitmap::integrate_last_block() {
  VAST_ASSERT(num_bits_ % word_type::width == 0);
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
  if (word_type::all_or_none(last_block)) {
    // Current dirty block turns out to be clean. (1)
    auto& marker = blocks_[last_marker_];
    auto clean_length = word_type::marker_num_clean(marker);
    auto last_block_type = !!last_block;
    if (blocks_after_marker == 1 && clean_length == 0) {
      // Adjust the type and counter of the existing marker.
      marker = word_type::marker_type(marker, last_block_type);
      marker = word_type::marker_num_clean(marker, 1);
      blocks_.pop_back();
    } else if (blocks_after_marker == 1
               && last_block_type == word_type::marker_type(marker)
               && clean_length != word_type::marker_clean_max) {
      // Just update the counter of the existing marker.
      marker = word_type::marker_num_clean(marker, clean_length + 1);
      blocks_.pop_back();
    } else {
      // Replace the last block with a new marker.
      auto m = word_type::marker_num_clean(word_type::marker_type(0, last_block_type), 1);
      last_block = m;
      last_marker_ = blocks_.size() - 1;
    }
  } else {
    // The current block is dirty.
    bump_dirty_count();
  }
}

void ewah_bitmap::bump_dirty_count() {
  VAST_ASSERT(num_bits_ % word_type::width == 0);
  auto& marker = blocks_[last_marker_];
  auto num_dirty = word_type::marker_num_dirty(marker);
  if (num_dirty == word_type::marker_dirty_max) {
    // We need a new marker: replace the current dirty block with a marker and
    // append a new block.
    auto& last_block = blocks_.back();
    auto dirty_block = last_block;
    last_block = word_type::marker_num_dirty(1);
    last_marker_ = blocks_.size() - 1;
    blocks_.push_back(dirty_block);
  } else {
    // We can still bump the counter of the current marker.
    marker = word_type::marker_num_dirty(marker, num_dirty + 1);
  }
}

bool operator==(const ewah_bitmap& x, const ewah_bitmap& y) {
  // If the block vector and the number of bits are equal, so must be the
  // marker by construction.
  return x.blocks_ == y.blocks_ && x.num_bits_ == y.num_bits_;
}

ewah_bitmap_range::ewah_bitmap_range(const ewah_bitmap& bm)
  : bm_{&bm} {
  if (!bm_->empty())
    scan();
}

bool ewah_bitmap_range::done() const {
  return next_ == bm_->blocks().size();
}

void ewah_bitmap_range::next() {
  VAST_ASSERT(!done());
  if (++next_ != bm_->blocks().size())
    scan();
}

void ewah_bitmap_range::scan() {
  VAST_ASSERT(next_ < bm_->blocks().size());
  auto block = bm_->blocks()[next_];
  if (next_ + 1 == bm_->blocks().size()) {
    // The ast block; always dirty.
    auto partial = bm_->size() % word_type::width;
    bits_ = {block, partial == 0 ? word_type::width : partial};
  } else if (num_dirty_ > 0) {
    // An intermediate dirty block.
    --num_dirty_;
    bits_ = {block, word_type::width};
  } else {
    // A marker.
    auto num_clean = word_type::marker_num_clean(block);
    num_dirty_ = word_type::marker_num_dirty(block);
    if (num_clean == 0) {
      // If the marker has no clean blocks, we can't record a fill sequence and
      // have to go to the next (literal) block.
      ++next_;
      scan();
    } else {
      auto data = word_type::marker_type(block) ? word_type::all : word_type::none;
      auto length = num_clean * word_type::width;
      // If no dirty blocks follow this marker and we have not reached the
      // final dirty block yet, we know that the next block must be a marker as
      // well and check whether we can incorporate it into this sequence.
      while (num_dirty_ == 0 && next_ + 2 < bm_->blocks().size()) {
        auto next_marker = bm_->blocks()[next_ + 1];
        auto next_type = word_type::marker_type(next_marker);
        if ((next_type && !data) || (!next_type && data))
          break; // not compatible with current run
        length += word_type::marker_num_clean(next_marker) * word_type::width;
        num_dirty_ = word_type::marker_num_dirty(next_marker);
        ++next_;
      }
      bits_ = {data, length};
    }
  }
}

ewah_bitmap_range bit_range(const ewah_bitmap& bm) {
  return ewah_bitmap_range{bm};
}

} // namespace vast

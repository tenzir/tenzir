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

#pragma once

#include "vast/bitmap_base.hpp"
#include "vast/bitvector.hpp"
#include "vast/word.hpp"

#include "vast/detail/operators.hpp"

namespace vast {

template <class Block>
struct ewah_word : word<Block> {
  /// The offset from the LSB which separates clean and dirty counters.
  static constexpr auto clean_dirty_divide = word<Block>::width / 2 - 1;

  /// The mask to apply to a marker word to extract the counter of dirty words.
  static constexpr auto marker_dirty_mask =
    ~(word<Block>::all << clean_dirty_divide);

  /// The maximum value of the counter of dirty words.
  static constexpr auto marker_dirty_max = marker_dirty_mask;

  /// The mask to apply to a marker word to extract the counter of clean words.
  static constexpr auto marker_clean_mask =
    ~(marker_dirty_mask | word<Block>::msb1);

  /// The maximum value of the counter of clean words.
  static constexpr auto marker_clean_max
    = marker_clean_mask >> clean_dirty_divide;

  /// Retrieves the type of the clean word in a marker word.
  static constexpr bool marker_type(Block block) {
    return (block & word<Block>::msb1) == word<Block>::msb1;
  }

  /// Sets the marker type.
  static constexpr Block marker_type(Block block, bool type) {
    return (block & ~word<Block>::msb1) | (type ? word<Block>::msb1 : 0);
  }

  /// Retrieves the number of clean words in a marker word.
  static constexpr Block marker_num_clean(Block block) {
    return (block & marker_clean_mask) >> clean_dirty_divide;
  }

  /// Sets the number of clean words in a marker word.
  static constexpr Block marker_num_clean(Block block, Block n) {
    return (block & ~marker_clean_mask) | (n << clean_dirty_divide);
  }

  /// Retrieves the number of dirty words following a marker word.
  static constexpr Block marker_num_dirty(Block block) {
    return block & marker_dirty_mask;
  }

  /// Sets the number of dirty words in a marker word.
  static constexpr Block marker_num_dirty(Block block, Block n) {
    return (block & ~marker_dirty_mask) | n;
  }
};

/// A bitmap encoded with the *Enhanced World-Aligned Hybrid (EWAH)* algorithm.
/// EWAH has two types of blocks: *marker* and *dirty*. The bits in a dirty
/// block are literally interpreted whereas the bits of a marker block have
/// following semantics, where W is the number of bits per block:
///
/// 1. Bits *[0,W/2)*: number of dirty words following clean bits
/// 2. Bits *[W/2,W-1)*: number of clean words
/// 3. MSB *W-1*: the type of the clean words
///
/// This implementation (internally) maintains the following invariants:
///
/// 1. The first block is a marker.
/// 2. The last block is always dirty.
///
class ewah_bitmap : public bitmap_base<ewah_bitmap>,
                    detail::equality_comparable<ewah_bitmap> {
public:
  using word_type = ewah_word<block_type>;
  using block_vector = std::vector<block_type>;

  ewah_bitmap() = default;

  explicit ewah_bitmap(size_type n, bool bit = false);

  // -- inspectors -----------------------------------------------------------

  bool empty() const;

  size_type size() const;

  const block_vector& blocks() const;

  // -- modifiers ------------------------------------------------------------

  void append_bit(bool bit);

  void append_bits(bool bit, size_type n);

  void append_block(block_type bits, size_type n = word_type::width);

  void flip();

  // -- concepts -------------------------------------------------------------

  friend bool operator==(const ewah_bitmap& x, const ewah_bitmap& y);

  template <class Inspector>
  friend auto inspect(Inspector&f, ewah_bitmap& bm) {
    return f(bm.blocks_, bm.last_marker_, bm.num_bits_);
  }

private:
  /// Incorporates the most recent (complete) dirty block.
  /// @pre `num_bits_ % word_type::width == 0`
  void integrate_last_block();

  /// Bumps up the dirty count of the current marker up or creates a new marker
  /// if the dirty count reached its maximum.
  /// @pre `num_bits_ % word_type::width == 0`
  void bump_dirty_count();

  block_vector blocks_;
  block_type last_marker_ = 0;
  size_type num_bits_ = 0;
};

class ewah_bitmap_range
  : public bit_range_base<ewah_bitmap_range, ewah_bitmap::block_type> {
public:
  using word_type = ewah_bitmap::word_type;

  ewah_bitmap_range() = default;

  explicit ewah_bitmap_range(const ewah_bitmap& bm);

  void next();
  bool done() const;

private:
  void scan();

  const ewah_bitmap* bm_;
  size_t next_ = 0;
  size_t num_dirty_ = 0;
};

ewah_bitmap_range bit_range(const ewah_bitmap& bm);

} // namespace vast



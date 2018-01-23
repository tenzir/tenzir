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

#ifndef VAST_WAH_BITMAP_HPP
#define VAST_WAH_BITMAP_HPP

#include "vast/bitmap_base.hpp"
#include "vast/bitvector.hpp"
#include "vast/word.hpp"

#include "vast/detail/operators.hpp"

namespace vast {

template <class Block>
struct wah_word : word<Block> {
  using typename word<Block>::size_type;

  // The number of bits that a literal word contains.
  static constexpr auto literal_word_size = word<Block>::width - 1;

  // The maximum length of a fill that a single word can represent.
  static constexpr size_type max_fill_words = word<Block>::all >> 2;

  // A mask for the fill bit in a fill word.
  static constexpr Block fill_mask = word<Block>::msb1 >> 1;

  // Retrieves the type of a fill.
  // Precondition: is_fill(block)
  static constexpr bool fill_type(Block block) {
    return (block & fill_mask) == fill_mask;
  }

  // Checks whether a block is a fill.
  static constexpr bool is_fill(Block block) {
    return block & word<Block>::msb1;
  }

  // Checks whether a block is a fill of a specific type.
  static constexpr bool is_fill(Block block, bool bit) {
    return is_fill(block) && fill_type(block) == bit;
  }

  // Counts the number literal words in a fill block.
  // Precondition: is_fill(block)
  static constexpr size_type fill_words(Block block) {
    return block & (word<Block>::all >> 2);
  }

  // Creates a fill word of a specific value and count.
  // Precondition: n <= max_fill_words
  static constexpr Block make_fill(bool bit, size_type n) {
    auto type = static_cast<Block>(bit) << (word<Block>::width - 2);
    return word<Block>::msb1 | type | Block{n};
  }
};

class wah_bitmap_range;

/// A bitmap encoded with the *World-Aligned Hybrid (WAH)* algorithm. WAH
/// features two types of words: literals and fills. Let *w* be the number of
/// bits of a word. If the MSB is 0, then the word is a literal word, i.e., the
/// remaining *w-1* bits are interpreted literally. Otherwise the second MSB
/// denotes the fill type and the remaining *w-2* bits represent a counter
/// value *n* to denote *n (w - 1)* bits.
///
/// The implementation must maintain the following invariant: there is always
/// an incomplete word at the end of the block sequence.
class wah_bitmap : public bitmap_base<wah_bitmap>,
                    detail::equality_comparable<wah_bitmap> {
  friend wah_bitmap_range;

public:
  using word_type = wah_word<block_type>;
  using block_vector = std::vector<block_type>;

  wah_bitmap() = default;

  wah_bitmap(size_type n, bool bit = false);

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

  friend bool operator==(const wah_bitmap& x, const wah_bitmap& y);

  template <class Inspector>
  friend auto inspect(Inspector&f, wah_bitmap& bm) {
    return f(bm.blocks_, bm.num_last_, bm.num_bits_);
  }

private:
  void merge_active_word();

  block_vector blocks_;
  size_type num_last_ = 0; // number of bits in last block
  size_type num_bits_ = 0;
};

class wah_bitmap_range
  : public bit_range_base<wah_bitmap_range, wah_bitmap::block_type> {
public:
  using word_type = wah_bitmap::word_type;

  wah_bitmap_range() = default;

  explicit wah_bitmap_range(const wah_bitmap& bm);

  void next();
  bool done() const;

private:
  void scan();

  const wah_bitmap* bm_;
  wah_bitmap::block_vector::const_iterator begin_;
  wah_bitmap::block_vector::const_iterator end_;
};

wah_bitmap_range bit_range(const wah_bitmap& bm);

} // namespace vast

#endif


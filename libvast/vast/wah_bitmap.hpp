#ifndef VAST_WAH_BITMAP_HPP
#define VAST_WAH_BITMAP_HPP

#include "vast/bitmap_base.hpp"
#include "vast/bitvector.hpp"
#include "vast/detail/operators.hpp"

namespace vast {

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
  using block_vector = std::vector<block_type>;

  wah_bitmap() = default;

  wah_bitmap(size_type n, bool bit = false);

  // -- inspectors -----------------------------------------------------------

  bool empty() const;

  size_type size() const;

  block_vector const& blocks() const;

  // -- modifiers ------------------------------------------------------------

  void append_bit(bool bit);

  void append_bits(bool bit, size_type n);

  void append_block(block_type bits, size_type n = word_type::width);

  void flip();

  // -- concepts -------------------------------------------------------------

  friend bool operator==(wah_bitmap const& x, wah_bitmap const& y);

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
  wah_bitmap_range() = default;

  explicit wah_bitmap_range(wah_bitmap const& bm);

  void next();
  bool done() const;

private:
  void scan();

  const wah_bitmap* bm_;
  wah_bitmap::block_vector::const_iterator begin_;
  wah_bitmap::block_vector::const_iterator end_;
};

wah_bitmap_range bit_range(wah_bitmap const& bm);

} // namespace vast

#endif


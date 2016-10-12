#ifndef VAST_NULL_BITMAP_HPP
#define VAST_NULL_BITMAP_HPP

#include "vast/bitmap_base.hpp"
#include "vast/bitvector.hpp"
#include "vast/detail/operators.hpp"

namespace vast {

class null_bitmap_range;

/// An uncompressed bitmap. Essentially, a null_bitmap lifts an append-only
/// ::bitvector into a bitmap type, enabling efficient block-level operations
/// and making it compatiable with algorithms that operate on bitmaps.
class null_bitmap : public bitmap_base<null_bitmap>,
                    detail::equality_comparable<null_bitmap> {
  friend null_bitmap_range;

public:
  using bitvector_type = bitvector<block_type>;

  null_bitmap() = default;

  // -- inspectors -----------------------------------------------------------

  bool empty() const;

  size_type size() const;

  // -- modifiers ------------------------------------------------------------

  bool append_bit(bool bit);

  bool append_bits(bool bit, size_type n);

  bool append_block(block_type bits, size_type n = word_type::width);

  // -- concepts -------------------------------------------------------------

  friend bool operator==(null_bitmap const& x, null_bitmap const& y);

  template <class Inspector>
  friend auto inspect(Inspector&f, null_bitmap& bm) {
    return f(bm.bitvector_);
  }

  friend null_bitmap_range bit_range(null_bitmap const& bm);

private:
  bitvector_type bitvector_;
};

class null_bitmap_range
  : public bit_range_base<null_bitmap_range, null_bitmap::block_type> {
public:
  explicit null_bitmap_range(null_bitmap const& bm);

  void next();
  bool done() const;

private:
  void scan();

  null_bitmap::bitvector_type const* bitvector_;
  typename null_bitmap::bitvector_type::block_vector::const_iterator block_;
};


} // namespace vast

#endif


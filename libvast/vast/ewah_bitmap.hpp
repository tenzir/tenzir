#ifndef VAST_EWAH_BITMAP_HPP
#define VAST_EWAH_BITMAP_HPP

#include "vast/bitmap_base.hpp"
#include "vast/bitvector.hpp"
#include "vast/detail/operators.hpp"

namespace vast {

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
  using block_vector = std::vector<block_type>;

  ewah_bitmap() = default;

  // -- inspectors -----------------------------------------------------------

  bool empty() const;

  size_type size() const;

  block_vector const& blocks() const;

  // -- modifiers ------------------------------------------------------------

  bool append_bit(bool bit);

  bool append_bits(bool bit, size_type n);

  bool append_block(block_type bits, size_type n = word_type::width);

  // -- concepts -------------------------------------------------------------

  friend bool operator==(ewah_bitmap const& x, ewah_bitmap const& y);

  template <class Inspector>
  friend auto inspect(Inspector&f, ewah_bitmap& bm) {
    return f(bm.blocks_, bm.last_marker_, bm.num_bits_);
  }

  // -- bitwise operations ----------------------------------------------------

  ewah_bitmap operator~() const;

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
  ewah_bitmap_range() = default;

  explicit ewah_bitmap_range(ewah_bitmap const& bm);

  void next();
  bool done() const;

private:
  void scan();

  ewah_bitmap const* bm_;
  size_t next_ = 0;
  size_t num_dirty_ = 0;
  size_t num_bits_ = 0;
};

ewah_bitmap_range bit_range(ewah_bitmap const& bm);

} // namespace vast

#endif


#ifndef VAST_BITSTREAM_H
#define VAST_BITSTREAM_H

#include <algorithm>

#include "vast/bitvector.h"
#include "vast/util/assert.h"
#include "vast/util/operators.h"
#include "vast/util/meta.h"
#include "vast/util/range.h"

namespace vast {

struct access;
class bitstream;
class null_bitstream;
class ewah_bitstream;

/// Determines whether a type is a valid bitstream.
template <typename Bitstream>
using is_bitstream = util::any<
  std::is_same<Bitstream, bitstream>,
  std::is_same<Bitstream, null_bitstream>,
  std::is_same<Bitstream, ewah_bitstream>
>;

// An abstraction over a contiguous sequence of bits in a bitstream. A bit
// sequence can have two types: a *fill* sequence representing a homogenous
// bits, typically greater than or equal to the block size, and a *literal*
// sequence representing bits from a single block, typically less than or
// equal to the block size.
struct bitseq {
  enum block_type { fill, literal };

  bool is_fill() const {
    return type == fill;
  }

  bool is_literal() const {
    return type == literal;
  }

  block_type type = literal;
  bitvector::size_type offset = 0;
  bitvector::block_type data = 0;
  bitvector::size_type length = 0;
};

/// The base class for all bitstream implementations.
template <typename Derived>
class bitstream_base {
  friend access;
  friend Derived;

public:
  using size_type = bitvector::size_type;
  using block_type = bitvector::block_type;
  static constexpr auto npos = bitvector::npos;
  static constexpr auto block_width = bitvector::block_width;
  static constexpr auto all_one = bitvector::all_one;
  static constexpr auto msb_one = bitvector::msb_one;

  Derived& operator&=(Derived const& other) {
    derived().bitwise_and(other);
    return static_cast<Derived&>(*this);
  }

  friend Derived operator&(Derived const& x, Derived const& y) {
    Derived d(x);
    return d &= y;
  }

  Derived& operator|=(Derived const& other) {
    derived().bitwise_or(other);
    return static_cast<Derived&>(*this);
  }

  friend Derived operator|(Derived const& x, Derived const& y) {
    Derived d(x);
    return d |= y;
  }

  Derived& operator^=(Derived const& other) {
    derived().bitwise_xor(other);
    return static_cast<Derived&>(*this);
  }

  friend Derived operator^(Derived const& x, Derived const& y) {
    Derived d(x);
    return d ^= y;
  }

  Derived&
  operator-=(Derived const& other) {
    derived().bitwise_subtract(other);
    return static_cast<Derived&>(*this);
  }

  friend Derived operator-(Derived const& x, Derived const& y) {
    Derived d(x);
    return d -= y;
  }

  /// Flips all bits, i.e., creates the complement bitstream.
  Derived& flip() {
    derived().bitwise_not();
    return static_cast<Derived&>(*this);
  }

  /// Flips all bits, i.e., creates the complement bitstream.
  Derived operator~() const {
    Derived d(derived());
    return d.flip();
  }

  /// Inspects a bit at a given position.
  /// @param i The bit position to check.
  /// @returns `true` if bit *i* is set.
  bool operator[](size_type i) const {
    return derived().at(i);
  }

  /// Retrieves the number of bits in the bitstream.
  /// @returns The number of total bits in the bitstream.
  size_type size() const {
    return derived().size_impl();
  }

  /// Retrieves the population count (aka. Hamming weight) of the bitstream.
  /// @returns The number of set bits.
  size_type count() const {
    return derived().count_impl();
  }

  /// Checks whether the bitstream has no bits.
  /// @returns `true` iff `size() > 0`
  bool empty() const {
    return derived().empty_impl();
  }

  /// Appends another bitstream.
  /// @param other The other bitstream.
  /// @returns `true` on success.
  bool append(Derived const& other) {
    if (other.empty())
      return true;
    if (empty()) {
      *this = other;
      return true;
    }
    if (std::numeric_limits<size_type>::max() - size() < other.size())
      return false;
    derived().append_impl(other);
    return true;
  }

  /// Appends a sequence of bits.
  /// @param n The number of bits to append.
  /// @param bit The bit value of the *n* bits to append.
  /// @returns `true` on success.
  bool append(size_type n, bool bit) {
    if (n == 0)
      return true;
    if (npos - n < size())
      return false;
    derived().append_impl(n, bit);
    return true;
  }

  /// Appends bits from a given block.
  /// @param block The block whose bits to append.
  /// @param bits The number of bits to take from *block*.
  /// @returns `true` on success.
  bool append_block(block_type block, size_type bits = block_width) {
    VAST_ASSERT(bits <= block_width);
    if (npos - bits < size())
      return false;
    derived().append_block_impl(block, bits);
    return true;
  }

  /// Appends a single bit.
  /// @param bit The bit value.
  /// @returns `true` on success.
  bool push_back(bool bit) {
    if (std::numeric_limits<size_type>::max() == size())
      return false;
    derived().push_back_impl(bit);
    return true;
  }

  /// Removes trailing zero bits.
  void trim() {
    derived().trim_impl();
  }

  /// Removes all bits from bitstream.
  /// @post `empty() == true`.
  void clear() noexcept {
    derived().clear_impl();
  }

  template <typename Hack = Derived>
  auto begin() const -> decltype(std::declval<Hack>().begin_impl()) {
    static_assert(std::is_same<Hack, Derived>::value, ":-P");
    return derived().begin_impl();
  }

  template <typename Hack = Derived>
  auto end() const -> decltype(std::declval<Hack>().end_impl()) {
    static_assert(std::is_same<Hack, Derived>::value, ":-P");
    return derived().end_impl();
  }

  /// Accesses the last bit of the bitstream.
  /// @returns The bit value of the last bit.
  bool back() const {
    VAST_ASSERT(!empty());
    return derived().back_impl();
  }

  /// Retrieves the position of the first one-bit.
  /// @returns The position of the first one-bit or bitstream::npos if no such
  /// position exists (i.e., if all bits are zero).
  size_type find_first() const {
    return derived().find_first_impl();
  }

  /// Finds the next one-bit after a given position.
  /// @param i The position after which to begin finding.
  /// @returns The position of the first one-bit after *i* or bitstream::npos
  /// if no such one-bit exists.
  size_type find_next(size_type i) const {
    auto r = derived().find_next_impl(i);
    VAST_ASSERT(r > i || r == npos);
    return r;
  }

  /// Retrieves the position of the last one-bit.
  /// @returns The position of the last one-bit or bitstream::npos if no such
  /// position exists (i.e., if all bits are zero).
  size_type find_last() const {
    return derived().find_last_impl();
  }

  /// Finds the previous one-bit before a given position.
  /// @param i The position before which to begin finding.
  /// @returns The position of the first one-bit before *i* or bitstream::npos
  /// if no such one-bit exists.
  size_type find_prev(size_type i) const {
    auto r = derived().find_prev_impl(i);
    VAST_ASSERT(r < i || r == npos);
    return r;
  }

  /// Checks whether a non-empty bitstream consists only of 0s.
  /// @returns `true` iff all bits in the bitstream are 0.
  /// @pre `! empty()`
  bool all_zeros() const {
    VAST_ASSERT(!empty());
    return find_first() == npos;
  }

  /// Checks whether a non-empty bitstream consists only of 1s.
  /// @returns `true` iff all bits in the bitstream are 1.
  /// @pre `! empty()`
  bool all_ones() const {
    VAST_ASSERT(!empty());
    return find_first() == size() - 1;
  }

  bitvector const& bits() const {
    return derived().bits_impl();
  }

protected:
  Derived& derived() {
    return *static_cast<Derived*>(this);
  }

  Derived const& derived() const {
    return *static_cast<Derived const*>(this);
  }
};

template <typename Derived>
typename bitstream_base<Derived>::size_type const bitstream_base<Derived>::npos;

namespace detail {

/// The base class for bit sequence ranges.
template <typename Derived>
class sequence_range_base
  : public util::range_facade<sequence_range_base<Derived>> {
protected:
  bool next() {
    return static_cast<Derived*>(this)->next_sequence(seq_);
  }

private:
  friend util::range_facade<sequence_range_base<Derived>>;

  bitseq const& state() const {
    return seq_;
  }

  bitseq seq_;
};

template <typename>
class bitstream_model;

} // namespace detail

/// An uncompressed bitstream that simply forwards all operations to its
/// underlying ::bitvector.
class null_bitstream : public bitstream_base<null_bitstream>,
                       util::totally_ordered<null_bitstream> {
  template <typename>
  friend class detail::bitstream_model;
  friend bitstream_base<null_bitstream>;
  friend access;
  friend bool operator==(null_bitstream const& x, null_bitstream const& y);
  friend bool operator<(null_bitstream const& x, null_bitstream const& y);

public:
  using const_iterator = class iterator
    : public util::iterator_adaptor<
        iterator,
        bitvector::const_ones_iterator,
        size_type,
        std::forward_iterator_tag,
        size_type
      > {
  public:
    iterator() = default;

    static iterator begin(null_bitstream const& null);
    static iterator end(null_bitstream const& null);

  private:
    friend util::iterator_access;

    explicit iterator(base_iterator const& i);
    auto dereference() const -> decltype(this->base().position());
  };

  class ones_range : public util::iterator_range<iterator> {
  public:
    explicit ones_range(null_bitstream const& bs)
      : util::iterator_range<iterator>{iterator::begin(bs), iterator::end(bs)} {
    }
  };

  class sequence_range : public detail::sequence_range_base<sequence_range> {
  public:
    explicit sequence_range(null_bitstream const& bs);

  private:
    friend detail::sequence_range_base<sequence_range>;

    bool next_sequence(bitseq& seq);

    bitvector const* bits_;
    size_type next_block_ = 0;
  };

  null_bitstream() = default;
  null_bitstream(size_type n, bool bit);

private:
  bool equals(null_bitstream const& other) const;
  void bitwise_not();
  void bitwise_and(null_bitstream const& other);
  void bitwise_or(null_bitstream const& other);
  void bitwise_xor(null_bitstream const& other);
  void bitwise_subtract(null_bitstream const& other);
  void append_impl(null_bitstream const& other);
  void append_impl(size_type n, bool bit);
  void append_block_impl(block_type block, size_type bits);
  void push_back_impl(bool bit);
  void trim_impl();
  void clear_impl() noexcept;
  bool at(size_type i) const;
  size_type size_impl() const;
  size_type count_impl() const;
  bool empty_impl() const;
  const_iterator begin_impl() const;
  const_iterator end_impl() const;
  bool back_impl() const;
  size_type find_first_impl() const;
  size_type find_next_impl(size_type i) const;
  size_type find_last_impl() const;
  size_type find_prev_impl(size_type i) const;
  bitvector const& bits_impl() const;

  bitvector bits_;
};

/// A bitstream encoded with the *Enhanced World-Aligned Hybrid (EWAH)*
/// algorithm. EWAH has two types of blocks: *marker* and *dirty*. The bits in
/// a dirty block are literally interpreted whereas the bits of a marker block
/// have are have following semantics, assuming N being the number of bits per
/// block:
///
///     1. Bits *[0,N/2)*: number of dirty words following clean bits
///     2. Bits *[N/2,N-1)*: number of clean words
///     3. MSB *N-1*: the type of the clean words
///
/// This implementation (internally) maintains the following invariants:
///
///     1. The first block is a marker.
///     2. The last block is always dirty.
class ewah_bitstream : public bitstream_base<ewah_bitstream>,
                       util::totally_ordered<ewah_bitstream> {
  template <typename>
  friend class detail::bitstream_model;
  friend bitstream_base<ewah_bitstream>;
  friend access;
  friend bool operator==(ewah_bitstream const& x, ewah_bitstream const& y);
  friend bool operator<(ewah_bitstream const& x, ewah_bitstream const& y);

public:
  using const_iterator = class iterator
    : public util::iterator_facade<
        iterator,
        size_type,
        std::forward_iterator_tag,
        size_type
      > {
  public:
    iterator() = default;

    static iterator begin(ewah_bitstream const& ewah);
    static iterator end(ewah_bitstream const& ewah);

  private:
    friend util::iterator_access;

    iterator(ewah_bitstream const& ewah);

    bool equals(iterator const& other) const;
    void increment();
    size_type dereference() const;

    void scan();

    static constexpr auto npos = bitvector::npos;

    ewah_bitstream const* ewah_ = nullptr;
    size_type pos_ = npos;
    size_type num_clean_ = 0;
    size_type num_dirty_ = 0; // Excludes the last dirty block.
    size_type idx_ = 0;
  };

  class ones_range : public util::iterator_range<iterator> {
  public:
    explicit ones_range(ewah_bitstream const& bs)
      : util::iterator_range<iterator>{iterator::begin(bs), iterator::end(bs)} {
    }
  };

  class sequence_range : public detail::sequence_range_base<sequence_range> {
  public:
    explicit sequence_range(ewah_bitstream const& bs);

  private:
    friend detail::sequence_range_base<sequence_range>;

    bool next_sequence(bitseq& seq);

    bitvector const* bits_;
    size_type next_block_ = 0;
    size_type num_dirty_ = 0;
    size_type num_bits_ = 0;
  };

  ewah_bitstream() = default;
  ewah_bitstream(size_type n, bool bit);
  ewah_bitstream(ewah_bitstream const&) = default;
  ewah_bitstream(ewah_bitstream&&) = default;
  ewah_bitstream& operator=(ewah_bitstream const&) = default;
  ewah_bitstream& operator=(ewah_bitstream&&) = default;

private:
  bool equals(ewah_bitstream const& other) const;
  void bitwise_not();
  void bitwise_and(ewah_bitstream const& other);
  void bitwise_or(ewah_bitstream const& other);
  void bitwise_xor(ewah_bitstream const& other);
  void bitwise_subtract(ewah_bitstream const& other);
  void append_impl(ewah_bitstream const& other);
  void append_impl(size_type n, bool bit);
  void append_block_impl(block_type block, size_type bits);
  void push_back_impl(bool bit);
  void trim_impl();
  void clear_impl() noexcept;
  bool at(size_type i) const;
  size_type size_impl() const;
  size_type count_impl() const;
  bool empty_impl() const;
  const_iterator begin_impl() const;
  const_iterator end_impl() const;
  bool back_impl() const;
  size_type find_first_impl() const;
  size_type find_next_impl(size_type i) const;
  size_type find_last_impl() const;
  size_type find_prev_impl(size_type i) const;
  bitvector const& bits_impl() const;

  /// The offset from the LSB which separates clean and dirty counters.
  static constexpr auto clean_dirty_divide = block_width / 2 - 1;

  /// The mask to apply to a marker word to extract the counter of dirty words.
  static constexpr auto marker_dirty_mask = ~(all_one << clean_dirty_divide);

  /// The maximum value of the counter of dirty words.
  static constexpr auto marker_dirty_max = marker_dirty_mask;

  /// The mask to apply to a marker word to extract the counter of clean words.
  static constexpr auto marker_clean_mask = ~(marker_dirty_mask | msb_one);

  /// The maximum value of the counter of clean words.
  static constexpr auto marker_clean_max
    = marker_clean_mask >> clean_dirty_divide;

  /// Retrieves the type of the clean word in a marker word.
  /// @param block The block to check.
  /// @returns `true` if *block* represents a sequence of 1s and `false` if 0s.
  static constexpr bool marker_type(block_type block) {
    return (block & msb_one) == msb_one;
  }

  static constexpr block_type marker_type(block_type block, bool type) {
    return (block & ~msb_one) | (type ? msb_one : 0);
  }

  /// Retrieves the number of clean words in a marker word.
  /// @param block The block to check.
  /// @returns The number of clean words in *block*.
  static constexpr block_type marker_num_clean(block_type block) {
    return (block & marker_clean_mask) >> clean_dirty_divide;
  }

  /// Sets the number of clean words in a marker word.
  /// @param block The block to check.
  /// @param n The new value for the number of clean words.
  /// @returns The updated block that has a new clean word length of *n*.
  static constexpr block_type marker_num_clean(block_type block, block_type n) {
    return (block & ~marker_clean_mask) | (n << clean_dirty_divide);
  }

  /// Retrieves the number of dirty words following a marker word.
  /// @param block The block to check.
  /// @returns The number of dirty words following *block*.
  static constexpr block_type marker_num_dirty(block_type block) {
    return block & marker_dirty_mask;
  }

  /// Sets the number of dirty words in a marker word.
  /// @param block The block to check.
  /// @param n The new value for the number of dirty words.
  /// @returns The updated block that has a new dirty word length of *n*.
  static constexpr block_type marker_num_dirty(block_type block, block_type n) {
    return (block & ~marker_dirty_mask) | n;
  }

  /// Incorporates the most recent (complete) dirty block.
  /// @pre `num_bits_ % block_width == 0`
  void integrate_last_block();

  /// Bumps up the dirty count of the current marker up or creates a new marker
  /// if the dirty count reached its maximum.
  /// @pre `num_bits_ % block_width == 0`
  void bump_dirty_count();

  size_type find_forward(size_type i) const;
  size_type find_backward(size_type i) const;

  bitvector bits_;
  size_type num_bits_ = 0;
  size_type last_marker_ = 0;
};

/// Applies a bitwise operation on two bitstreams.
/// The algorithm traverses the two bitstreams side by side.
///
/// @param lhs The LHS of the operation.
///
/// @param rhs The RHS of the operation
///
/// @param fill_lhs A boolean flag that controls the algorithm behavior after
/// one sequence has reached its end. If `true`, the algorithm will append the
/// remaining bits of *lhs* to the result iff *lhs* is the longer bitstream. If
/// `false`, the algorithm returns the result after the first sequence has
/// reached an end.
///
/// @param fill_rhs The same as *fill_lhs*, except that it concerns *rhs*.
///
/// @param op The bitwise operation as block-wise lambda, e.g., for XOR:
///
///     [](block_type lhs, block_type rhs) { return lhs ^ rhs; }
///
/// @returns The result of a bitwise operation between *lhs* and *rhs*
/// according to *op*.
template <typename Bitstream, typename Operation>
Bitstream apply(Bitstream const& lhs, Bitstream const& rhs, bool fill_lhs,
                bool fill_rhs, Operation op) {
  auto rx = typename Bitstream::sequence_range{lhs};
  auto ry = typename Bitstream::sequence_range{rhs};
  auto ix = rx.begin();
  auto iy = ry.begin();
  // Check corner cases.
  if (ix == rx.end() && iy == ry.end())
    return {};
  if (ix == rx.end())
    return rhs;
  if (iy == ry.end())
    return lhs;
  // Initialize result.
  Bitstream result;
  auto first = std::min(ix->offset, iy->offset);
  if (first > 0)
    result.append(first, false);
  // Iterate.
  auto lx = ix->length;
  auto ly = iy->length;
  while (ix != rx.end() && iy != rx.end()) {
    auto min = std::min(lx, ly);
    auto block = op(ix->data, iy->data);
    if (ix->is_fill() && iy->is_fill()) {
      result.append(min, block != 0);
      lx -= min;
      ly -= min;
    } else if (ix->is_fill()) {
      result.append_block(block);
      lx -= Bitstream::block_width;
      ly = 0;
    } else if (iy->is_fill()) {
      result.append_block(block);
      ly -= Bitstream::block_width;
      lx = 0;
    } else {
      result.append_block(block, std::max(lx, ly));
      lx = ly = 0;
    }
    if (lx == 0 && ++ix != rx.end())
      lx = ix->length;
    if (ly == 0 && ++iy != ry.end())
      ly = iy->length;
  }
  if (fill_lhs) {
    while (ix != rx.end()) {
      if (ix->is_fill())
        result.append(lx, ix->data);
      else
        result.append_block(ix->data, ix->length);

      if (++ix != rx.end())
        lx = ix->length;
    }
  }
  if (fill_rhs) {
    while (iy != ry.end()) {
      if (iy->is_fill())
        result.append(ly, iy->data);
      else
        result.append_block(iy->data, iy->length);

      if (++iy != ry.end())
        ly = iy->length;
    }
  }
  // If the result has not yet been filled with the remaining bits of either
  // LHS or RHS, we have to fill it up with zeros. This is necessary, for
  // example, to ensure that the complement of the result can still be used in
  // further bitwise operations with bitstreams having the size of
  // max(size(LHS), size(RHS)).
  result.append(std::max(lhs.size(), rhs.size()) - result.size(), false);
  return result;
}

template <typename Bitstream>
Bitstream and_(Bitstream const& lhs, Bitstream const& rhs) {
  using block_type = typename Bitstream::block_type;
  return apply(lhs, rhs, false, false,
               [](block_type x, block_type y) { return x & y; });
}

template <typename Bitstream>
Bitstream or_(Bitstream const& lhs, Bitstream const& rhs) {
  using block_type = typename Bitstream::block_type;
  return apply(lhs, rhs, true, true,
               [](block_type x, block_type y) { return x | y; });
}

template <typename Bitstream>
Bitstream xor_(Bitstream const& lhs, Bitstream const& rhs) {
  using block_type = typename Bitstream::block_type;
  return apply(lhs, rhs, true, true,
               [](block_type x, block_type y) { return x ^ y; });
}

template <typename Bitstream>
Bitstream nand_(Bitstream const& lhs, Bitstream const& rhs) {
  using block_type = typename Bitstream::block_type;
  return apply(lhs, rhs, true, false,
               [](block_type x, block_type y) { return x & ~y; });
}

template <typename Bitstream>
Bitstream nor_(Bitstream const& lhs, Bitstream const& rhs) {
  using block_type = typename Bitstream::block_type;
  return apply(lhs, rhs, true, true,
               [](block_type x, block_type y) { return x | ~y; });
}

} // namespace vast

#endif

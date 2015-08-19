#ifndef VAST_BITVECTOR_H
#define VAST_BITVECTOR_H

#include <limits>
#include <string>
#include <vector>

#include "vast/trial.h"
#include "vast/util/assert.h"
#include "vast/util/operators.h"
#include "vast/util/iterator.h"

namespace vast {

struct access;

/// A vector of bits having similar semantics as a `std::vector<bool>`.
class bitvector : util::totally_ordered<bitvector> {
  friend access;

public:
  // TODO: make configurable
  using block_type = uint64_t;
  using size_type = uint64_t;

  /// Bits per block.
  static constexpr block_type block_width
    = std::numeric_limits<block_type>::digits;

  /// One past the last addressable bit index; analogue to an `end` iterator.
  static constexpr size_type npos = ~size_type{0};

  /// A block with all 0s.
  static constexpr block_type all_zero = block_type{0};

  /// A block with all 1s.
  static constexpr block_type all_one = ~all_zero;

  /// A block with only its MSB set to 1.
  static constexpr block_type msb_one = ~(all_one >> 1);

public:
  /// An lvalue proxy for single bits.
  class reference {
    friend class bitvector;
    void operator&() = delete;

    /// Constructs a bit from a block.
    /// @param block The block to look at.
    /// @param i The bit position within *block*.
    reference(block_type& block, block_type i);

  public:
    reference& flip();
    operator bool() const;
    bool operator~() const;
    reference& operator=(bool x);
    reference& operator=(reference const& other);
    reference& operator|=(bool x);
    reference& operator&=(bool x);
    reference& operator^=(bool x);
    reference& operator-=(bool x);

  private:
    block_type& block_;
    block_type const mask_;
  };

  /// Unlike the reference type, a const_reference does not need lvalue
  /// semantics and can thus represent simply a boolean (bit) value.
  using const_reference = bool;

  /// The base class for iterators which inspect every single bit.
  template <typename Bitvector>
  class bit_iterator_base
    : public util::iterator_facade<
        bit_iterator_base<Bitvector>,
        bool,
        std::random_access_iterator_tag,
        std::conditional_t<
          std::is_const<Bitvector>::value, const_reference, reference
        >,
        size_type
      > {
  public:
    using reverse_iterator = std::reverse_iterator<bit_iterator_base>;

    bit_iterator_base() = default;

    static bit_iterator_base begin(Bitvector& bits) {
      return bit_iterator_base{bits};
    }

    static bit_iterator_base end(Bitvector& bits) {
      return bit_iterator_base{bits, bits.size()};
    }

    static reverse_iterator rbegin(Bitvector& bits) {
      return reverse_iterator{end(bits)};
    }

    static reverse_iterator rend(Bitvector& bits) {
      return reverse_iterator{begin(bits)};
    }

  private:
    friend util::iterator_access;

    bit_iterator_base(Bitvector& bits, size_type off = 0)
      : bits_{&bits}, i_{off} {
      VAST_ASSERT(bits_);
      VAST_ASSERT(!bits_->empty());
    }

    bool equals(bit_iterator_base const& other) const {
      return i_ == other.i_;
    }

    void increment() {
      VAST_ASSERT(i_ != npos);
      ++i_;
    }

    void decrement() {
      VAST_ASSERT(i_ != npos);
      --i_;
    }

    void advance(size_type n) {
      i_ += n;
    }

    auto dereference() const
      -> std::conditional_t<
           std::is_const<Bitvector>::value, const_reference, reference
         > {
      VAST_ASSERT(bits_);
      VAST_ASSERT(i_ != npos);
      return const_cast<Bitvector&>(*bits_)[i_];
    }

    Bitvector* bits_ = nullptr;
    size_type i_ = npos;
  };

  using bit_iterator = bit_iterator_base<bitvector>;
  using const_bit_iterator = bit_iterator_base<bitvector const>;

  /// The base class for iterators which inspect 1 bits only.
  template <typename Bitvector>
  class ones_iterator_base
    : public util::iterator_facade<
        ones_iterator_base<Bitvector>,
        bool,
        std::bidirectional_iterator_tag,
        std::conditional_t<
          std::is_const<Bitvector>::value, const_reference, reference
        >,
        size_type
      > {
  public:
    using reverse_iterator = std::reverse_iterator<ones_iterator_base>;

    ones_iterator_base() = default;

    static ones_iterator_base begin(Bitvector& bits) {
      return ones_iterator_base{bits, true};
    }

    static ones_iterator_base end(Bitvector&) {
      return ones_iterator_base{};
    }

    static reverse_iterator rbegin(Bitvector& bits) {
      return reverse_iterator{ones_iterator_base{bits, false}};
    }

    static reverse_iterator rend(Bitvector& bits) {
      return reverse_iterator{begin(bits)};
    }

    size_type position() const {
      return i_;
    }

  private:
    friend util::iterator_access;

    ones_iterator_base(Bitvector& bits, bool forward) : bits_{&bits} {
      VAST_ASSERT(bits_);
      VAST_ASSERT(!bits_->empty());
      i_ = forward ? bits_->find_first() : bits_->find_last();
    }

    bool equals(ones_iterator_base const& other) const {
      return i_ == other.i_;
    }

    void increment() {
      VAST_ASSERT(bits_);
      i_ = bits_->find_next(i_);
    }

    void decrement() {
      VAST_ASSERT(bits_);
      VAST_ASSERT(i_ != npos);
      i_ = bits_->find_prev(i_);
    }

    auto dereference() const
      -> std::conditional_t<
           std::is_const<Bitvector>::value, const_reference, reference
         > {
      VAST_ASSERT(bits_);
      VAST_ASSERT(i_ != npos);
      return const_cast<Bitvector&>(*bits_)[i_];
    }

    Bitvector* bits_ = nullptr;
    size_type i_ = npos;
  };

  using ones_iterator = ones_iterator_base<bitvector>;
  using const_ones_iterator = ones_iterator_base<bitvector const>;

  /// Computes the block index for a given bit position.
  static constexpr size_type block_index(size_type i) {
    return i / block_width;
  }

  /// Computes the bit index within a given block for a given bit position.
  static constexpr block_type bit_index(size_type i) {
    return i % block_width;
  }

  /// Computes the bitmask block to extract a bit a given bit position.
  static constexpr block_type bit_mask(size_type i) {
    return block_type(1) << bit_index(i);
  }

  /// Computes the number of blocks needed to represent a given number of
  /// bits.
  /// @param bits the number of bits.
  /// @returns The number of blocks to represent *bits* number of bits.
  static constexpr size_type bits_to_blocks(size_type bits) {
    return bits / block_width + static_cast<size_type>(bits % block_width != 0);
  }

  /// Flips the bits of a block beginning at a given position
  /// @param block The block to flip.
  /// @param start The position within the block where to start the flipping.
  /// @returns The complement of *block*, flipped beginning at *start*.
  /// @pre `start < block_width`
  static size_type flip(block_type block, size_type start);

  /// Computes the number of 1-bits in a given block (aka. *population count*).
  /// @param block The block to inspect.
  /// @returns The number of 1-bits in *block*.
  static size_type count(block_type block);

  /// Computes the bit position first 1-bit in a given block.
  /// @param block The block to inspect.
  /// @returns The bit position where *block* has its first bit set to 1.
  /// @pre At least one bit in *block* must be 1.
  static size_type lowest_bit(block_type block);

  /// Computes the bit position last 1-bit in a given block.
  /// @param block The block to inspect.
  /// @returns The bit position where *block* has its last bit set to 1.
  /// @pre At least one bit in *block* must be 1.
  static size_type highest_bit(block_type block);

  /// Finds the next bit in a block starting from a given offset.
  /// @param block The block to inspect.
  /// @param i The offset from the LSB where to begin searching.
  /// @returns The index in the block where the next 1-bit after *i* occurs or
  ///          `npos` if no such bit exists.
  static size_type next_bit(block_type block, size_type i);

  /// Finds the previous bit in a block starting from a given offset.
  /// @param block The block to inspect.
  /// @param i The offset from the LSB where to begin searching.
  /// @returns The index in the block where the 1-bit before *i* occurs or
  ///          `npos` if no such bit exists.
  static size_type prev_bit(block_type block, size_type i);

  /// Constructs an empty bit vector.
  bitvector();

  /// Constructs a bit vector of a given size.
  /// @param size The number of bits.
  /// @param value The value for each bit.
  explicit bitvector(size_type size, bool value = false);

  /// Constructs a bit vector from a sequence of blocks.
  template <typename InputIterator>
  bitvector(InputIterator first, InputIterator last) {
    bits_.insert(bits_.end(), first, last);
    num_bits_ = bits_.size() * block_width;
  }

  bitvector(bitvector const&) = default;
  bitvector(bitvector&&) = default;
  bitvector& operator=(bitvector const&) = default;
  bitvector& operator=(bitvector&&) = default;

  friend bool operator==(bitvector const& x, bitvector const& y);
  friend bool operator<(bitvector const& x, bitvector const& y);

  //
  // Bitwise operations
  //
  bitvector operator~() const;
  bitvector operator<<(size_type n) const;
  bitvector operator>>(size_type n) const;
  bitvector& operator<<=(size_type n);
  bitvector& operator>>=(size_type n);
  bitvector& operator&=(bitvector const& other);
  bitvector& operator|=(bitvector const& other);
  bitvector& operator^=(bitvector const& other);
  bitvector& operator-=(bitvector const& other);
  friend bitvector operator&(bitvector const& x, bitvector const& y);
  friend bitvector operator|(bitvector const& x, bitvector const& y);
  friend bitvector operator^(bitvector const& x, bitvector const& y);
  friend bitvector operator-(bitvector const& x, bitvector const& y);

  //
  // Basic operations
  //

  /// Appends the bits in a given block.
  /// @param block The block containing bits to append.
  /// @param bits The number of bits to append (starting from the LSB).
  /// @pre `bits <= block_width`
  void append(block_type block, size_type bits = block_width);

  /// Appends a bit vector to this instance.
  /// @param other The other bit vector to append.
  void append(bitvector const& other);

  /// Appends a single bit to the end of the bit vector.
  /// @param bit The value of the bit.
  void push_back(bool bit);

  /// Clears all bits in the bitvector.
  void clear() noexcept;

  /// Resizes the bit vector to a new number of bits.
  /// @param n The new number of bits of the bit vector.
  /// @param value The bit value of new values, if the vector expands.
  void resize(size_type n, bool value = false);

  /// Sets a bit at a specific position to a given value.
  /// @param i The bit position.
  /// @param bit The value assigned to position *i*.
  /// @returns A reference to the bit vector instance.
  bitvector& set(size_type i, bool bit = true);

  /// Sets all bits to 1.
  /// @returns A reference to the bit vector instance.
  bitvector& set();

  /// Resets a bit at a specific position, i.e., sets it to 0.
  /// @param i The bit position.
  /// @returns A reference to the bit vector instance.
  bitvector& reset(size_type i);

  /// Sets all bits to 0.
  /// @returns A reference to the bit vector instance.
  bitvector& reset();

  /// Toggles/flips a bit at a specific position.
  /// @param i The bit position.
  /// @returns A reference to the bit vector instance.
  bitvector& toggle(size_type i);

  /// Generates the complement bitvector start at a given position.
  /// @param start The bit position where to start flipping.
  /// @returns A reference to the bit vector instance.
  /// @pre `start < size()`
  bitvector& flip(size_type start = 0);

  /// Retrieves a single bit.
  /// @param i The bit position.
  /// @returns A mutable reference to the bit at position *i*.
  reference operator[](size_type i);

  /// Retrieves a single bit.
  /// @param i The bit position.
  /// @returns A const-reference to the bit at position *i*.
  const_reference operator[](size_type i) const;

  /// Appends blocks from an iterator range.
  /// @tparam Iterator A forward iterator.
  /// @param first Points to the first block of the range.
  /// @param last Points to the one past the last block of the range.
  template <typename Iterator>
  void block_append(Iterator first, Iterator last) {
    if (first == last)
      return;
    auto delta = last - first;
    num_bits_ += block_width * delta;
    bits_.reserve(blocks() + delta);
    auto extra = extra_bits();
    if (extra == 0) {
      bits_.insert(bits_.end(), first, last);
      return;
    }
    bits_.back() |= (*first << extra);
    do {
      auto blk = *first++ >> (block_width - extra);
      bits_.push_back(blk | (first == last ? 0 : *first << extra));
    } while (first != last);
  }

  /// Counts the number of 1-bits in the bit vector.
  /// Also known as *population count* or *Hamming weight*.
  /// @returns The number of bits set to 1.
  size_type count() const;

  /// Retrieves the number of bits the bitvector consist of.
  /// @returns The length of the bit vector in bits.
  size_type size() const;

  /// Checks whether the bit vector is empty.
  /// @returns `true` iff the bitvector has zero length.
  bool empty() const;

  /// Retrieves the number of active bits in the last block.
  /// @returns The number of active bits the last block.
  block_type extra_bits() const;

  /// Finds the bit position of of the first 1-bit.
  ///
  /// @returns The position of the first bit that equals to one or `npos` if no
  /// such bit exists.
  size_type find_first() const;

  /// Finds the next 1-bit from a given starting position.
  /// @param i The index where to start looking forward.
  /// @returns The position of the first bit that equals to 1 after position
  ///          *i*  or `npos` if no such bit exists.
  size_type find_next(size_type i) const;

  /// Finds the bit position of of the last 1-bit.
  /// @returns The position of the last bit that equals to one or `npos` if no
  ///          such bit exists.
  size_type find_last() const;

  /// Finds the previous 1-bit from a given starting position.
  /// @param i The index where to start looking backward.
  /// @returns The position of the first bit that equals to 1 before position
  ///          *i* or `npos` if no such bit exists.
  size_type find_prev(size_type i) const;

  /// Reserves space in the underlying block vector.
  /// @param n The number of bits to reserve space for.
  void reserve(size_type n);

  //
  // Block-based API
  //

  /// Retrieves the number of blocks of the underlying storage.
  /// @param The number of blocks that represent `size()` bits.
  size_type blocks() const;

  /// Retrieves an entire block at a given block index.
  /// @param *b* The block index.
  /// @returns The *b*th block.
  /// @pre *b < blocks()*.
  block_type block(size_type b) const;

  /// Retrieves an entire block at a given block index.
  /// @param *b* The block index.
  /// @returns The *b*th block.
  /// @pre *b < blocks()*.
  block_type& block(size_type b);

  /// Retrieves an entire block at a given bit position.
  /// @param *i* The bit position.
  /// @returns The entire block corresponding to bit position *i*.
  /// @pre *i < bits()*
  block_type block_at_bit(size_type i) const;

  /// Retrieves an entire block at a given bit position.
  /// @param *i* The bit position.
  /// @returns The entire block corresponding to bit position *i*.
  /// @pre *i < bits()*
  block_type& block_at_bit(size_type i);

  /// Retrieves the first block of the bitvector.
  /// @returns The first block.
  /// @pre *! empty()*
  block_type first_block() const;

  /// Retrieves the first block of the bitvector.
  /// @returns The first block.
  /// @pre *! empty()*
  block_type& first_block();

  /// Retrieves the last block of the bitvector.
  /// @returns The last block.
  /// @pre *! empty()*
  block_type last_block() const;

  /// Retrieves the last block of the bitvector.
  /// @returns The last block.
  /// @pre *! empty()*
  block_type& last_block();

private:
  // If the number of bits in the vector are not not a multiple of
  // bitvector::block_width, then the last block exhibits unused bits which
  // this function resets.
  void zero_unused_bits();

  /// Looks forward for the first 1-bit starting at a given position.
  /// @param i The block index to start looking.
  /// @returns The block index of the first 1-bit starting from *i* or
  ///          `bitvector::npos` if no 1-bit exists.
  size_type find_forward(size_type i) const;

  /// Looks backward for the first 1-bit starting at a given position.
  /// @param i The block index to start looking backward.
  /// @returns The block index of the first 1-bit going backward from *i* or
  ///          `bitvector::npos` if no 1-bit exists.
  size_type find_backward(size_type i) const;

  std::vector<block_type> bits_;
  size_type num_bits_;
};

} // namespace vast

#endif

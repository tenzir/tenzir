#ifndef VAST_BITVECTOR_H
#define VAST_BITVECTOR_H

#include <iterator>
#include <limits>
#include <vector>
#include "vast/io/fwd.h"

namespace vast {

// Forward declarations.
class bitvector;
std::string to_string(bitvector const&, bool, size_t);

/// A vector of bits.
class bitvector
{
  friend std::string to_string(bitvector const&, bool, size_t);

public:
  typedef size_t block_type;
  typedef size_t size_type;
  static size_type constexpr npos = static_cast<size_type>(-1);
  static block_type constexpr bits_per_block = 
    std::numeric_limits<block_type>::digits;

public:
  /// An lvalue proxy for single bits.
  class reference
  {
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
  typedef bool const_reference;

  /// Constructs an empty bit vector.
  bitvector();

  /// Constructs a bit vector of a given size.
  /// @param size The number of bits.
  /// @param value The value for each bit.
  explicit bitvector(size_type size, bool value = false);

  /// Constructs a bit vector from a sequence of blocks.
  template <typename InputIterator>
  bitvector(InputIterator first, InputIterator last)
  {
    bits_.insert(bits_.end(), first, last);
    num_bits_ = bits_.size() * bits_per_block;
  }

  /// Copy-constructs a bit vector.
  /// @param other The bit vector to copy.
  bitvector(bitvector const& other);

  /// Move-constructs a bit vector.
  /// @param other The bit vector to move.
  bitvector(bitvector&& other);

  /// Assigns another bit vector to this instance.
  /// @param other The RHS of the assignment.
  bitvector& operator=(bitvector other);

  /// Swaps two bit vectors.
  friend void swap(bitvector x, bitvector y);

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
  // Relational operators
  //
  friend bool operator==(bitvector const& x, bitvector const& y);
  friend bool operator!=(bitvector const& x, bitvector const& y);
  friend bool operator<(bitvector const& x, bitvector const& y);

  //
  // Basic operations
  //
  /// Appends the bits in a sequence of values.
  /// @tparam Iterator A forward iterator.
  /// @param first An iterator pointing to the first element of the sequence.
  /// @param last An iterator pointing to one past the last element of the
  /// sequence.
  template <
    typename Iterator,
    typename std::enable_if<
      std::is_same<
        typename std::iterator_traits<Iterator>::iterator_category,
        std::forward_iterator_tag
      >::value
    >::type = 0
  >
  void append(Iterator first, Iterator last)
  {
    if (first == last)
      return;

    auto excess = extra_bits();
    auto delta = std::distance(first, last);
    bits_.reserve(blocks() + delta);
    if (excess == 0)
    {
      bits_.back() |= (*first << excess);
      do
      {
        auto b = *first++ >> (bits_per_block - excess);
        bits_.push_back(b | (first == last ? 0 : *first << excess));
      } while (first != last);
    }
    else
    {
      bits_.insert(bits_.end(), first, last);
    }

    num_bits_ += bits_per_block * delta;
  }

  /// Appends the bits in a given block.
  /// @param block The block containing bits to append.
  void append(block_type block);

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
  /// @return A reference to the bit vector instance.
  bitvector& set(size_type i, bool bit = true);

  /// Sets all bits to 1.
  /// @return A reference to the bit vector instance.
  bitvector& set();

  /// Resets a bit at a specific position, i.e., sets it to 0.
  /// @param i The bit position.
  /// @return A reference to the bit vector instance.
  bitvector& reset(size_type i);

  /// Sets all bits to 0.
  /// @return A reference to the bit vector instance.
  bitvector& reset();

  /// Toggles/flips a bit at a specific position.
  /// @param i The bit position.
  /// @return A reference to the bit vector instance.
  bitvector& flip(size_type i);

  /// Computes the complement 
  /// @return A reference to the bit vector instance.
  bitvector& flip();

  /// Retrieves a single bit.
  /// @param i The bit position.
  /// @return A mutable reference to the bit at position *i*.
  reference operator[](size_type i);

  /// Retrieves a single bit.
  /// @param i The bit position.
  /// @return A const-reference to the bit at position *i*.
  const_reference operator[](size_type i) const;

  /// Counts the number of 1-bits in the bit vector. Also known as *population
  /// count* or *Hamming weight*.
  /// @return The number of bits set to 1.
  size_type count() const;

  /// Retrieves the number of blocks of the underlying storage.
  /// @param The number of blocks that represent `size()` bits.
  size_type blocks() const;

  /// Retrieves the number of bits the bitvector consist of.
  /// @return The length of the bit vector in bits.
  size_type size() const;

  /// Checks whether the bit vector is empty.
  /// @return `true` iff the bitvector has zero length.
  bool empty() const;

  /// Finds the bit position of of the first 1-bit.
  ///
  /// @return The position of the first bit that equals to one or `npos` if no
  /// such bit exists.
  size_type find_first() const;

  /// Finds the next 1-bit from a given starting position.
  ///
  /// @param i The index where to start looking.
  ///
  /// @return The position of the first bit that equals to 1 after position
  /// *i*  or `npos` if no such bit exists.
  size_type find_next(size_type i) const;

private:
  /// Computes the block index for a given bit position.
  static size_type constexpr block_index(size_type i)
  {
    return i / bits_per_block;
  }

  /// Computes the bit index within a given block for a given bit position.
  static block_type constexpr bit_index(size_type i)
  {
    return i % bits_per_block;
  }

  /// Computes the bitmask block to extract a bit a given bit position.
  static block_type constexpr bit_mask(size_type i)
  {
    return block_type(1) << bit_index(i);
  }

  /// Computes the number of blocks needed to represent a given number of
  /// bits.
  /// @param bits the number of bits.
  /// @return The number of blocks to represent *bits* number of bits.
  static size_type constexpr bits_to_blocks(size_type bits)
  {
    return bits / bits_per_block 
      + static_cast<size_type>(bits % bits_per_block != 0);
  }

  /// Computes the bit position first 1-bit in a given block.
  /// @param block The block to inspect.
  /// @return The bit position where *block* has its first bit set to 1.
  static size_type lowest_bit(block_type block);

  /// Computes the number of excess/unused bits in the bit vector.
  block_type extra_bits() const;

  // If the number of bits in the vector are not not a multiple of
  // bitvector::bits_per_block, then the last block exhibits unused bits which
  // this function resets.
  void zero_unused_bits();

  /// Looks for the first 1-bit starting at a given position.
  ///
  /// @param i The block index to start looking.
  ///
  /// @return The block index of the first 1-bit starting from *i* or
  /// `bitvector::npos` if no 1-bit exists.
  size_type find_from(size_type i) const;

  void serialize(io::serializer& sink);
  void deserialize(io::deserializer& source);

  std::vector<block_type> bits_;
  size_type num_bits_;
};

/// Converts a bitvector to a `std::string`.
///
/// @param b The bitvector to convert.
///
/// @param msb_to_lsb The order of display. If `true`, display bits from MSB to
/// LSB and in the reverse order otherwise.
///
/// @param all Indicates whether to include also the unused bits of the last
/// block if the number of `b.size()` is not a multiple of
/// `bitvector::bits_per_block`.
///
/// @param cut_off Specifies a maximum size on the output. If 0, no cutting
/// occurs.
///
/// @return A `std::string` representation of *b*.
std::string to_string(bitvector const& b,
                      bool msb_to_lsb = true,
                      bool all = false,
                      size_t cut_off = 0);

} // namespace vast

#endif

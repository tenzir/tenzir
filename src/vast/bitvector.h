#ifndef VAST_BITVECTOR_H
#define VAST_BITVECTOR_H

#include <cassert>
#include <iterator>
#include <limits>
#include <string>
#include <vector>
#include "vast/fwd.h"
#include "vast/traits.h"
#include "vast/util/operators.h"
#include "vast/util/iterator.h"
#include "vast/util/print.h"

namespace vast {

/// A vector of bits.
class bitvector : util::totally_ordered<bitvector>,
                  util::printable<bitvector>
{
public:
  using block_type = size_t;
  using size_type = size_t;

  /// Bits per block.
  static constexpr block_type block_width =
    std::numeric_limits<block_type>::digits;

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
  using const_reference = bool;

  /// The base class for iterators which inspect every single bit.
  template <typename Bitvector>
  class bit_iterator_base :
    public util::iterator_facade<
             bit_iterator_base<Bitvector>,
             std::random_access_iterator_tag,
             bool,
             Conditional<std::is_const<Bitvector>, const_reference, reference>,
             size_type
           >
  {
  public:
    using reverse_iterator = std::reverse_iterator<bit_iterator_base>;

    bit_iterator_base() = default;

    static bit_iterator_base begin(Bitvector& bits)
    {
      return bit_iterator_base{bits};
    }

    static bit_iterator_base end(Bitvector& bits)
    {
      return bit_iterator_base{bits, bits.size()};
    }

    static reverse_iterator rbegin(Bitvector& bits)
    {
      return reverse_iterator{end(bits)};
    }

    static reverse_iterator rend(Bitvector& bits)
    {
      return reverse_iterator{begin(bits)};
    }

  private:
    friend util::iterator_access;

    bit_iterator_base(Bitvector& bits, size_type off = 0)
      : bits_{&bits},
        i_{off}
    {
      assert(bits_);
      assert(! bits_->empty());
    }

    bool equals(bit_iterator_base const& other) const
    {
      return i_ == other.i_;
    }

    void increment()
    {
      assert(i_ != npos);
      ++i_;
    }

    void decrement()
    {
      assert(i_ != npos);
      --i_;
    }

    void advance(size_type n)
    {
      i_ += n;
    }

    auto dereference() const
      -> Conditional<std::is_const<Bitvector>, const_reference, reference>
    {
      assert(bits_);
      assert(i_ != npos);
      return const_cast<Bitvector&>(*bits_)[i_];
    }

    Bitvector* bits_ = nullptr;
    size_type i_ = npos;
  };

  using bit_iterator = bit_iterator_base<bitvector>;
  using const_bit_iterator = bit_iterator_base<bitvector const>;

  /// The base class for iterators which inspect 1 bits only.
  template <typename Bitvector>
  class ones_iterator_base :
    public util::iterator_facade<
             ones_iterator_base<Bitvector>,
             std::bidirectional_iterator_tag,
             bool,
             Conditional<std::is_const<Bitvector>, const_reference, reference>,
             size_type
           >
  {
  public:
    using reverse_iterator = std::reverse_iterator<ones_iterator_base>;

    ones_iterator_base() = default;

    static ones_iterator_base begin(Bitvector& bits)
    {
      return ones_iterator_base{bits, true};
    }

    static ones_iterator_base end(Bitvector&)
    {
      return ones_iterator_base{};
    }

    static reverse_iterator rbegin(Bitvector& bits)
    {
      return reverse_iterator{ones_iterator_base{bits, false}};
    }

    static reverse_iterator rend(Bitvector& bits)
    {
      return reverse_iterator{begin(bits)};
    }

    size_type position() const
    {
      return i_;
    }

  private:
    friend util::iterator_access;

    ones_iterator_base(Bitvector& bits, bool forward)
      : bits_{&bits}
    {
      assert(bits_);
      assert(! bits_->empty());
      i_ = forward ? bits_->find_first() : bits_->find_last();
    }

    bool equals(ones_iterator_base const& other) const
    {
      return i_ == other.i_;
    }

    void increment()
    {
      assert(bits_);
      i_ = bits_->find_next(i_);
    }

    void decrement()
    {
      assert(bits_);
      assert(i_ != npos);
      i_ = bits_->find_prev(i_);
    }

    auto dereference() const
      -> Conditional<std::is_const<Bitvector>, const_reference, reference>
    {
      assert(bits_);
      assert(i_ != npos);
      return const_cast<Bitvector&>(*bits_)[i_];
    }

    Bitvector* bits_ = nullptr;
    size_type i_ = npos;
  };

  using ones_iterator = ones_iterator_base<bitvector>;
  using const_ones_iterator = ones_iterator_base<bitvector const>;

  /// Computes the block index for a given bit position.
  static constexpr size_type block_index(size_type i)
  {
    return i / block_width;
  }

  /// Computes the bit index within a given block for a given bit position.
  static constexpr block_type bit_index(size_type i)
  {
    return i % block_width;
  }

  /// Computes the bitmask block to extract a bit a given bit position.
  static constexpr block_type bit_mask(size_type i)
  {
    return block_type(1) << bit_index(i);
  }

  /// Computes the number of blocks needed to represent a given number of
  /// bits.
  /// @param bits the number of bits.
  /// @returns The number of blocks to represent *bits* number of bits.
  static constexpr size_type bits_to_blocks(size_type bits)
  {
    return bits / block_width + static_cast<size_type>(bits % block_width != 0);
  }

  /// Computes the number of 1-bits in a given block (aka. *population count*).
  /// @param block The block to inspect.
  /// @returns The number of 1-bits in *block*.
  static size_type count(block_type block);

  /// Computes the bit position first 1-bit in a given block.
  /// @param block The block to inspect.
  /// @returns The bit position where *block* has its first bit set to 1.
  static size_type lowest_bit(block_type block);

  /// Computes the bit position last 1-bit in a given block.
  /// @param block The block to inspect.
  /// @returns The bit position where *block* has its last bit set to 1.
  static size_type highest_bit(block_type block);

  /// Finds the next bit in a block starting from a given offset.
  ///
  /// @param block The block to inspect.
  ///
  /// @param i The offset from the LSB where to begin searching.
  ///
  /// @returns The index in the block where the next 1-bit after *i* occurs or
  /// `npos` if no such bit exists.
  static size_type next_bit(block_type block, size_type i);

  /// Finds the previous bit in a block starting from a given offset.
  ///
  /// @param block The block to inspect.
  ///
  /// @param i The offset from the LSB where to begin searching.
  ///
  /// @returns The index in the block where the 1-bit before *i* occurs or
  /// `npos` if no such bit exists.
  static size_type prev_bit(block_type block, size_type i);

  /// Prints a single block.
  /// @param out The iterator to print to.
  /// @param block The block to print to *out*.
  /// @param msb Whether to start from the MSB or not.
  /// @param begin The offset in *block* from the LSB where to start printing.
  /// @param end One past the last offset to print.
  template <typename Iterator>
  static bool print(Iterator& out,
                    block_type block,
                    bool msb = true,
                    block_type begin = 0,
                    block_type end = block_width)
  {
    if (msb)
      while (begin < end)
        *out++ = (block & (block_type(1) << (end - begin++ - 1))) ? '1' : '0';
    else
      while (begin < end)
        *out++ = (block & (block_type(1) << begin++)) ? '1' : '0';

    return true;
  }

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
    num_bits_ = bits_.size() * block_width;
  }

  bitvector(bitvector const&) = default;
  bitvector(bitvector&&) = default;
  bitvector& operator=(bitvector const&) = default;
  bitvector& operator=(bitvector&&) = default;

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
  /// Appends the bits in a sequence of values.
  /// @tparam Iterator A forward iterator.
  /// @param first An iterator pointing to the first element of the sequence.
  /// @param last An iterator pointing to one past the last element of the
  /// sequence.
  template <
    typename Iterator,
    typename = EnableIf<
      std::is_same<
        typename std::iterator_traits<Iterator>::iterator_category,
        std::forward_iterator_tag
      >
    >
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
        auto bv = *first++ >> (block_width - excess);
        bits_.push_back(bv | (first == last ? 0 : *first << excess));
      } while (first != last);
    }
    else
    {
      bits_.insert(bits_.end(), first, last);
    }

    num_bits_ += block_width * delta;
  }

  /// Appends the bits in a given block.
  /// @param block The block containing bits to append.
  /// @param bits The number of bits (from the LSB) to append.
  /// @pre `bits <= block_width`
  void append(block_type block, size_type bits = block_width);

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
  bitvector& flip(size_type i);

  /// Computes the complement
  /// @returns A reference to the bit vector instance.
  bitvector& flip();

  /// Retrieves a single bit.
  /// @param i The bit position.
  /// @returns A mutable reference to the bit at position *i*.
  reference operator[](size_type i);

  /// Retrieves a single bit.
  /// @param i The bit position.
  /// @returns A const-reference to the bit at position *i*.
  const_reference operator[](size_type i) const;

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

  /// Counts the number of 1-bits in the bit vector. Also known as *population
  /// count* or *Hamming weight*.
  /// @returns The number of bits set to 1.
  size_type count() const;

  /// Retrieves the number of blocks of the underlying storage.
  /// @param The number of blocks that represent `size()` bits.
  size_type blocks() const;

  /// Retrieves the number of bits the bitvector consist of.
  /// @returns The length of the bit vector in bits.
  size_type size() const;

  /// Checks whether the bit vector is empty.
  /// @returns `true` iff the bitvector has zero length.
  bool empty() const;

  /// Computes the number of bits of the last block.
  /// @returns The number of bits the last block occupies.
  block_type extra_bits() const;

  /// Finds the bit position of of the first 1-bit.
  ///
  /// @returns The position of the first bit that equals to one or `npos` if no
  /// such bit exists.
  size_type find_first() const;

  /// Finds the next 1-bit from a given starting position.
  ///
  /// @param i The index where to start looking forward.
  ///
  /// @returns The position of the first bit that equals to 1 after position
  /// *i*  or `npos` if no such bit exists.
  size_type find_next(size_type i) const;

  /// Finds the bit position of of the last 1-bit.
  ///
  /// @returns The position of the last bit that equals to one or `npos` if no
  /// such bit exists.
  size_type find_last() const;

  /// Finds the previous 1-bit from a given starting position.
  ///
  /// @param i The index where to start looking backward.
  ///
  /// @returns The position of the first bit that equals to 1 before position
  /// *i* or `npos` if no such bit exists.
  size_type find_prev(size_type i) const;

private:
  // If the number of bits in the vector are not not a multiple of
  // bitvector::block_width, then the last block exhibits unused bits which
  // this function resets.
  void zero_unused_bits();

  /// Looks forward for the first 1-bit starting at a given position.
  ///
  /// @param i The block index to start looking.
  ///
  /// @returns The block index of the first 1-bit starting from *i* or
  /// `bitvector::npos` if no 1-bit exists.
  size_type find_forward(size_type i) const;

  /// Looks backward for the first 1-bit starting at a given position.
  ///
  /// @param i The block index to start looking backward.
  ///
  /// @returns The block index of the first 1-bit going backward from *i* or
  /// `bitvector::npos` if no 1-bit exists.
  size_type find_backward(size_type i) const;

  std::vector<block_type> bits_;
  size_type num_bits_;

private:
  friend access;

  void serialize(serializer& sink) const;
  void deserialize(deserializer& source);

  /// Prints a bitvector.
  ///
  /// @param out An iterator modeling the OutputIterator concept.
  ///
  /// @param msb: The order of display. If `true`, display bits from MSB
  /// to LSB and in the reverse order otherwise.
  ///
  /// @param all: Indicates whether to include also the unused bits of the last
  /// block if the number of `b.size()` is not a multiple of
  /// `bitvector::block_width`.
  ///
  /// @param max: Specifies a maximum size on the output. If 0, no cutting
  /// occurs.
  template <typename Iterator>
  bool
  print(Iterator& out, bool msb = true, bool all = false, size_t max = 0) const
  {
    std::string str;
    auto str_size = all ? bitvector::block_width * blocks() : size();
    if (max == 0 || str_size <= max)
    {
      str.assign(str_size, '0');
    }
    else
    {
      str.assign(max + 2, '0');
      str[max + 0] = '.';
      str[max + 1] = '.';
      str_size = max;
    }

    for (size_type i = 0; i < std::min(str_size, size()); ++i)
      if (operator[](i))
        str[msb ? str_size - i - 1 : i] = '1';

    out = std::copy(str.begin(), str.end(), out);
    return true;
  }

  friend bool operator==(bitvector const& x, bitvector const& y);
  friend bool operator<(bitvector const& x, bitvector const& y);
};

} // namespace vast

#endif

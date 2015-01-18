#ifndef VAST_BITSTREAM_H
#define VAST_BITSTREAM_H

#include <algorithm>
#include "vast/bitvector.h"
#include "vast/serialization/arithmetic.h"
#include "vast/serialization/container.h"
#include "vast/serialization/pointer.h"
#include "vast/util/operators.h"
#include "vast/util/range.h"

namespace vast {

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
struct bitseq
{
  enum block_type { fill, literal };

  bool is_fill() const
  {
    return type == fill;
  }

  bool is_literal() const
  {
    return type == literal;
  }

  block_type type = literal;
  bitvector::size_type offset = 0;
  bitvector::block_type data = 0;
  bitvector::size_type length = 0;
};

/// The base class for all bitstream implementations.
template <typename Derived>
class bitstream_base
{
  friend Derived;

public:
  using size_type = bitvector::size_type;
  using block_type = bitvector::block_type;
  static constexpr auto npos = bitvector::npos;
  static constexpr auto block_width = bitvector::block_width;
  static constexpr auto all_one = bitvector::all_one;
  static constexpr auto msb_one = bitvector::msb_one;

  Derived& operator&=(Derived const& other)
  {
    derived().bitwise_and(other);
    return static_cast<Derived&>(*this);
  }

  friend Derived operator&(Derived const& x, Derived const& y)
  {
    Derived d(x);
    return d &= y;
  }

  Derived& operator|=(Derived const& other)
  {
    derived().bitwise_or(other);
    return static_cast<Derived&>(*this);
  }

  friend Derived operator|(Derived const& x, Derived const& y)
  {
    Derived d(x);
    return d |= y;
  }

  Derived& operator^=(Derived const& other)
  {
    derived().bitwise_xor(other);
    return static_cast<Derived&>(*this);
  }

  friend Derived operator^(Derived const& x, Derived const& y)
  {
    Derived d(x);
    return d ^= y;
  }

  Derived& operator-=(Derived const& other)
  {
    derived().bitwise_subtract(other);
    return static_cast<Derived&>(*this);
  }

  friend Derived operator-(Derived const& x, Derived const& y)
  {
    Derived d(x);
    return d -= y;
  }

  /// Flips all bits, i.e., creates the complement bitstream.
  Derived& flip()
  {
    derived().bitwise_not();
    return static_cast<Derived&>(*this);
  }

  /// Flips all bits, i.e., creates the complement bitstream.
  Derived operator~() const
  {
    Derived d(derived());
    return d.flip();
  }

  /// Inspects a bit at a given position.
  /// @param i The bit position to check.
  /// @returns `true` if bit *i* is set.
  bool operator[](size_type i) const
  {
    return derived().at(i);
  }

  /// Retrieves the number of bits in the bitstream.
  /// @returns The number of total bits in the bitstream.
  size_type size() const
  {
    return derived().size_impl();
  }

  /// Retrieves the population count (aka. Hamming weight) of the bitstream.
  /// @returns The number of set bits.
  size_type count() const
  {
    return derived().count_impl();
  }

  /// Checks whether the bitstream has no bits.
  /// @returns `true` iff `size() > 0`
  bool empty() const
  {
    return derived().empty_impl();
  }

  /// Appends a sequence of bits.
  /// @param n The number of bits to append.
  /// @param bit The bit value of the *n* bits to append.
  /// @returns `true` on success.
  bool append(size_type n, bool bit)
  {
    if (n == 0 || npos - n < size())
      return false;
    derived().append_impl(n, bit);
    return true;
  }

  /// Appends bits from a given block.
  /// @param block The block whose bits to append.
  /// @param bits The number of bits to take from *block*.
  /// @returns `true` on success.
  bool append_block(block_type block, size_type bits = block_width)
  {
    assert(bits <= block_width);
    if (npos - bits < size())
      return false;
    derived().append_block_impl(block, bits);
    return true;
  }

  /// Appends a single bit.
  /// @param bit The bit value.
  /// @returns `true` on success.
  bool push_back(bool bit)
  {
    if (std::numeric_limits<size_type>::max() == size())
      return false;
    derived().push_back_impl(bit);
    return true;
  }

  /// Removes trailing zero bits.
  void trim()
  {
    derived().trim_impl();
  }

  /// Removes all bits from bitstream.
  /// @post `empty() == true`.
  void clear() noexcept
  {
    derived().clear_impl();
  }

  template <typename Hack = Derived>
  auto begin() const
    -> decltype(std::declval<Hack>().begin_impl())
  {
    static_assert(std::is_same<Hack, Derived>::value, ":-P");
    return derived().begin_impl();
  }

  template <typename Hack = Derived>
  auto end() const
    -> decltype(std::declval<Hack>().end_impl())
  {
    static_assert(std::is_same<Hack, Derived>::value, ":-P");
    return derived().end_impl();
  }

  /// Accesses the last bit of the bitstream.
  /// @returns The bit value of the last bit.
  bool back() const
  {
    assert(! empty());
    return derived().back_impl();
  }

  /// Retrieves the position of the first one-bit.
  /// @returns The position of the first one-bit or bitstream::npos if no such
  /// position exists (i.e., if all bits are zero).
  size_type find_first() const
  {
    return derived().find_first_impl();
  }

  /// Finds the next one-bit after a given position.
  /// @param i The position after which to begin finding.
  /// @returns The position of the first one-bit after *i* or bitstream::npos
  /// if no such one-bit exists.
  size_type find_next(size_type i) const
  {
    auto r = derived().find_next_impl(i);
    assert(r > i || r == npos);
    return r;
  }

  /// Retrieves the position of the last one-bit.
  /// @returns The position of the last one-bit or bitstream::npos if no such
  /// position exists (i.e., if all bits are zero).
  size_type find_last() const
  {
    return derived().find_last_impl();
  }

  /// Finds the previous one-bit before a given position.
  /// @param i The position before which to begin finding.
  /// @returns The position of the first one-bit before *i* or bitstream::npos
  /// if no such one-bit exists.
  size_type find_prev(size_type i) const
  {
    auto r = derived().find_prev_impl(i);
    assert(r < i || r == npos);
    return r;
  }

  /// Checks whether the bitstream consists only of zero.
  /// @returns `true` iff all bits in the bitstream are zero.
  bool all_zero() const
  {
    return find_first() == npos;
  }

  bitvector const& bits() const
  {
    return derived().bits_impl();
  }

protected:
  Derived& derived()
  {
    return *static_cast<Derived*>(this);
  }

  Derived const& derived() const
  {
    return *static_cast<Derived const*>(this);
  }

private:
  friend access;

  void serialize(serializer& sink) const
  {
    derived().serialize(sink);
  }

  void deserialize(deserializer& source) const
  {
    derived().deserialize(source);
  }
};

template <typename Derived>
typename bitstream_base<Derived>::size_type const bitstream_base<Derived>::npos;

namespace detail {

/// The base class for bit sequence ranges.
template <typename Derived>
class sequence_range_base
  : public util::range_facade<sequence_range_base<Derived>>
{
protected:
  bool next()
  {
    return static_cast<Derived*>(this)->next_sequence(seq_);
  }

private:
  friend util::range_facade<sequence_range_base<Derived>>;

  bitseq const& state() const
  {
    return seq_;
  }

  bitseq seq_;
};

/// The concept for bitstreams.
class bitstream_concept
{
public:
  using size_type = bitvector::size_type;
  using block_type = bitvector::block_type;

private:
  class iterator_concept
  {
  public:
    iterator_concept() = default;
    virtual ~iterator_concept() = default;
    virtual std::unique_ptr<iterator_concept> copy() const = 0;

    virtual bool equals(iterator_concept const& other) const = 0;
    virtual void increment() = 0;
    virtual size_type dereference() const = 0;
  };

  /// A concrete model for a specific bitstream iterator.
  template <typename Iterator>
  class iterator_model : public iterator_concept
  {
  public:
    iterator_model() = default;

    iterator_model(Iterator&& i)
      : iterator_{std::move(i)}
    {
    }

  private:
    virtual std::unique_ptr<iterator_concept> copy() const final
    {
      return std::make_unique<iterator_model>(*this);
    }

    virtual bool equals(iterator_concept const& other) const final
    {
      if (typeid(*this) != typeid(other))
        throw std::runtime_error{"incompatible iterator types"};

      return iterator_ ==
          static_cast<iterator_model const&>(other).iterator_;
    }

    virtual void increment() final
    {
      ++iterator_;
    }

    virtual size_type dereference() const final
    {
      return *iterator_;
    }

    Iterator iterator_;
  };

public:
  using const_iterator = class iterator
    : public util::iterator_facade<
               iterator, std::forward_iterator_tag, size_type, size_type
            >
  {
  public:
    iterator() = default;

    template <
      typename Iterator,
      typename = util::disable_if_same_or_derived_t<iterator, Iterator>
    >
    iterator(Iterator&& i)
      : concept_{
          new iterator_model<std::decay_t<Iterator>>{std::forward<Iterator>(i)}}
    {
    }

    iterator(iterator const& other);
    iterator(iterator&& other);
    iterator& operator=(iterator const& other);
    iterator& operator=(iterator&& other);

  private:
    friend util::iterator_access;

    bool equals(iterator const& other) const;
    void increment();
    size_type dereference() const;

    std::unique_ptr<iterator_concept> concept_;
  };

  virtual ~bitstream_concept() = default;
  virtual std::unique_ptr<bitstream_concept> copy() const = 0;

  // Interface as required by bitstream_base<T>.
  virtual bool equals(bitstream_concept const& other) const = 0;
  virtual void bitwise_not() = 0;
  virtual void bitwise_and(bitstream_concept const& other) = 0;
  virtual void bitwise_or(bitstream_concept const& other) = 0;
  virtual void bitwise_xor(bitstream_concept const& other) = 0;
  virtual void bitwise_subtract(bitstream_concept const& other) = 0;
  virtual void append_impl(size_type n, bool bit) = 0;
  virtual void append_block_impl(block_type block, size_type bits) = 0;
  virtual void push_back_impl(bool bit) = 0;
  virtual void trim_impl() = 0;
  virtual void clear_impl() noexcept = 0;
  virtual bool at(size_type i) const = 0;
  virtual size_type size_impl() const = 0;
  virtual size_type count_impl() const = 0;
  virtual bool empty_impl() const = 0;
  virtual const_iterator begin_impl() const = 0;
  virtual const_iterator end_impl() const = 0;
  virtual bool back_impl() const = 0;
  virtual size_type find_first_impl() const = 0;
  virtual size_type find_next_impl(size_type i) const = 0;
  virtual size_type find_last_impl() const = 0;
  virtual size_type find_prev_impl(size_type i) const = 0;
  virtual bitvector const& bits_impl() const = 0;

protected:
  bitstream_concept() = default;

private:
  friend access;
  friend bitstream;
  virtual void serialize(serializer& sink) const = 0;
  virtual void deserialize(deserializer& source) = 0;
};


/// A concrete bitstream.
template <typename Bitstream>
class bitstream_model : public bitstream_concept,
                        util::equality_comparable<bitstream_model<Bitstream>>
{
  Bitstream const& cast(bitstream_concept const& c) const
  {
    if (typeid(c) != typeid(*this))
      throw std::bad_cast();
    return static_cast<bitstream_model const&>(c).bitstream_;
  }

  Bitstream& cast(bitstream_concept& c)
  {
    if (typeid(c) != typeid(*this))
      throw std::bad_cast();
    return static_cast<bitstream_model&>(c).bitstream_;
  }

  friend bool operator==(bitstream_model const& x, bitstream_model const& y)
  {
    return x.bitstream_ == y.bitstream_;
  }

public:
  bitstream_model() = default;

  bitstream_model(Bitstream bs)
    : bitstream_(std::move(bs))
  {
  }

  virtual std::unique_ptr<bitstream_concept> copy() const final
  {
    return std::make_unique<bitstream_model>(*this);
  }

  virtual bool equals(bitstream_concept const& other) const final
  {
    return bitstream_.equals(cast(other));
  }

  virtual void bitwise_not() final
  {
    bitstream_.bitwise_not();
  }

  virtual void bitwise_and(bitstream_concept const& other) final
  {
    bitstream_.bitwise_and(cast(other));
  }

  virtual void bitwise_or(bitstream_concept const& other) final
  {
    bitstream_.bitwise_or(cast(other));
  }

  virtual void bitwise_xor(bitstream_concept const& other) final
  {
    bitstream_.bitwise_xor(cast(other));
  }

  virtual void bitwise_subtract(bitstream_concept const& other) final
  {
    bitstream_.bitwise_subtract(cast(other));
  }

  virtual void append_impl(size_type n, bool bit) final
  {
    bitstream_.append_impl(n, bit);
  }

  virtual void append_block_impl(block_type block, size_type bits) final
  {
    bitstream_.append_block_impl(block, bits);
  }

  virtual void push_back_impl(bool bit) final
  {
    bitstream_.push_back_impl(bit);
  }

  virtual void trim_impl() final
  {
    bitstream_.trim_impl();
  }

  virtual void clear_impl() noexcept final
  {
    bitstream_.clear_impl();
  }

  virtual bool at(size_type i) const final
  {
    return bitstream_.at(i);
  }

  virtual size_type size_impl() const final
  {
    return bitstream_.size_impl();
  }

  virtual size_type count_impl() const final
  {
    return bitstream_.count_impl();
  }

  virtual bool empty_impl() const final
  {
    return bitstream_.empty_impl();
  }

  virtual const_iterator begin_impl() const final
  {
    return const_iterator{bitstream_.begin_impl()};
  }

  virtual const_iterator end_impl() const final
  {
    return const_iterator{bitstream_.end_impl()};
  }

  virtual bool back_impl() const final
  {
    return bitstream_.back_impl();
  }

  virtual size_type find_first_impl() const final
  {
    return bitstream_.find_first_impl();
  }

  virtual size_type find_next_impl(size_type i) const final
  {
    return bitstream_.find_next_impl(i);
  }

  virtual size_type find_last_impl() const final
  {
    return bitstream_.find_last_impl();
  }

  virtual size_type find_prev_impl(size_type i) const final
  {
    return bitstream_.find_prev_impl(i);
  }

  virtual bitvector const& bits_impl() const final
  {
    return bitstream_.bits_impl();
  }

private:
  Bitstream bitstream_;

private:
  friend access;

  virtual void serialize(serializer& sink) const final
  {
    sink << bitstream_;
  }

  virtual void deserialize(deserializer& source) final
  {
    source >> bitstream_;
  }
};

} // namespace detail

/// A polymorphic bitstream with value semantics.
class bitstream : public bitstream_base<bitstream>,
                  util::equality_comparable<bitstream>
{
public:
  using iterator = detail::bitstream_concept::iterator;
  using const_iterator = detail::bitstream_concept::const_iterator;

  bitstream() = default;
  bitstream(bitstream const& other);
  bitstream(bitstream&& other);

  template <
    typename Bitstream,
    typename = util::disable_if_same_or_derived_t<bitstream, Bitstream>
  >
  explicit bitstream(Bitstream&& bs)
    : concept_{
        new detail::bitstream_model<std::decay_t<Bitstream>>{
            std::forward<Bitstream>(bs)}}
  {
  }

  bitstream& operator=(bitstream const& other);
  bitstream& operator=(bitstream&& other);

  explicit operator bool() const;

private:
  friend bitstream_base<bitstream>;

  bool equals(bitstream const& other) const;
  void bitwise_not();
  void bitwise_and(bitstream const& other);
  void bitwise_or(bitstream const& other);
  void bitwise_xor(bitstream const& other);
  void bitwise_subtract(bitstream const& other);
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

  std::unique_ptr<detail::bitstream_concept> concept_;

private:
  friend access;

  void serialize(serializer& sink) const;
  void deserialize(deserializer& source);

  template <typename Iterator>
  friend trial<void> print(bitstream const& bs, Iterator&& out)
  {
    return print(bs.bits(), out, false, false, 0);
  }

  friend bool operator==(bitstream const& x, bitstream const& y);
};

/// An uncompressed bitstream that simply forwards all operations to its
/// underlying ::bitvector.
class null_bitstream : public bitstream_base<null_bitstream>,
                       util::totally_ordered<null_bitstream>
{
public:
  using const_iterator = class iterator
    : public util::iterator_adaptor<
        iterator,
        bitvector::const_ones_iterator,
        std::forward_iterator_tag,
        size_type,
        size_type
      >
  {
  public:
    iterator() = default;

    static iterator begin(null_bitstream const& null);
    static iterator end(null_bitstream const& null);

  private:
    friend util::iterator_access;

    explicit iterator(base_iterator const& i);
    auto dereference() const -> decltype(this->base().position());
  };

  class ones_range : public util::iterator_range<iterator>
  {
  public:
    explicit ones_range(null_bitstream const& bs)
      : util::iterator_range<iterator>{iterator::begin(bs), iterator::end(bs)}
    {
    }
  };

  class sequence_range : public detail::sequence_range_base<sequence_range>
  {
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
  template <typename>
  friend class detail::bitstream_model;
  friend bitstream_base<null_bitstream>;

  bool equals(null_bitstream const& other) const;
  void bitwise_not();
  void bitwise_and(null_bitstream const& other);
  void bitwise_or(null_bitstream const& other);
  void bitwise_xor(null_bitstream const& other);
  void bitwise_subtract(null_bitstream const& other);
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

private:
  friend access;
  void serialize(serializer& sink) const;
  void deserialize(deserializer& source);

  template <typename Iterator>
  friend trial<void> print(null_bitstream const& bs, Iterator&& out)
  {
    // We print NULL bitstreams from LSB to MSB to underline the stream
    // character.
    return print(bs.bits(), out, false, false, 0);
  }

  friend bool operator==(null_bitstream const& x, null_bitstream const& y);
  friend bool operator<(null_bitstream const& x, null_bitstream const& y);
};

/// A bitstream encoded using the *Enhanced World-Aligned Hybrid (EWAH)*
/// algorithm.
///
/// @note This implementation internally maintains the following invariants:
///
///   1. The first block is a marker.
///   2. The last block is always dirty.
class ewah_bitstream : public bitstream_base<ewah_bitstream>,
                       util::totally_ordered<ewah_bitstream>
{
public:
  using const_iterator = class iterator
    : public util::iterator_facade<
               iterator, std::forward_iterator_tag, size_type, size_type
             >
  {
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

  class ones_range : public util::iterator_range<iterator>
  {
  public:
    explicit ones_range(ewah_bitstream const& bs)
      : util::iterator_range<iterator>{iterator::begin(bs), iterator::end(bs)}
    {
    }
  };

  class sequence_range : public detail::sequence_range_base<sequence_range>
  {
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
  template <typename>
  friend class detail::bitstream_model;
  friend bitstream_base<ewah_bitstream>;

  bool equals(ewah_bitstream const& other) const;
  void bitwise_not();
  void bitwise_and(ewah_bitstream const& other);
  void bitwise_or(ewah_bitstream const& other);
  void bitwise_xor(ewah_bitstream const& other);
  void bitwise_subtract(ewah_bitstream const& other);
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
  static constexpr auto marker_clean_max =
    marker_clean_mask >> clean_dirty_divide;

  /// Retrieves the type of the clean word in a marker word.
  /// @param block The block to check.
  /// @returns `true` if *block* represents a sequence of 1s and `false` if 0s.
  static constexpr bool marker_type(block_type block)
  {
    return (block & msb_one) == msb_one;
  }

  static constexpr block_type marker_type(block_type block, bool type)
  {
    return (block & ~msb_one) | (type ? msb_one : 0);
  }

  /// Retrieves the number of clean words in a marker word.
  /// @param block The block to check.
  /// @returns The number of clean words in *block*.
  static constexpr block_type marker_num_clean(block_type block)
  {
    return (block & marker_clean_mask) >> clean_dirty_divide;
  }

  /// Sets the number of clean words in a marker word.
  /// @param block The block to check.
  /// @param n The new value for the number of clean words.
  /// @returns The updated block that has a new clean word length of *n*.
  static constexpr block_type marker_num_clean(block_type block, block_type n)
  {
    return (block & ~marker_clean_mask) | (n << clean_dirty_divide);
  }

  /// Retrieves the number of dirty words following a marker word.
  /// @param block The block to check.
  /// @returns The number of dirty words following *block*.
  static constexpr block_type marker_num_dirty(block_type block)
  {
    return block & marker_dirty_mask;
  }

  /// Sets the number of dirty words in a marker word.
  /// @param block The block to check.
  /// @param n The new value for the number of dirty words.
  /// @returns The updated block that has a new dirty word length of *n*.
  static constexpr block_type marker_num_dirty(block_type block, block_type n)
  {
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

private:
  friend access;
  void serialize(serializer& sink) const;
  void deserialize(deserializer& source);

  template <typename Iterator>
  friend trial<void> print(ewah_bitstream const& bs, Iterator&& out)
  {
    for (size_t i = 0; i < bs.bits_.blocks(); ++i)
    {
      if (i != bs.bits_.blocks() - 1)
      {
        if (! bitvector::print(out, bs.bits_.block(i)))
          return error{"failed to print block ", i};

        *out++ = '\n';
      }
      else
      {
        auto remaining = bs.num_bits_ % block_width;
        if (remaining == 0)
          remaining = block_width;

        for (size_t i = 0; i < block_width - remaining; ++i)
          *out++ = ' ';

        if (! bitvector::print(out, bs.bits_.block(i), true, 0, remaining))
          return error{"failed to print block ", i};
      }
    }

    return nothing;
  }

  friend bool operator==(ewah_bitstream const& x, ewah_bitstream const& y);
  friend bool operator<(ewah_bitstream const& x, ewah_bitstream const& y);
};

/// Performs a bitwise operation on two bitstreams.
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
Bitstream apply(Bitstream const& lhs, Bitstream const& rhs,
                bool fill_lhs, bool fill_rhs, Operation op)
{
  auto rx = typename Bitstream::sequence_range{lhs};
  auto ry = typename Bitstream::sequence_range{rhs};
  auto ix = rx.begin();
  auto iy = ry.begin();

  if (ix == rx.end() && iy == ry.end())
    return {};
  if (ix == rx.end())
    return rhs;
  if (iy == ry.end())
    return lhs;

  Bitstream result;
  auto first = std::min(ix->offset, iy->offset);
  if (first > 0)
    result.append(first, false);

  auto lx = ix->length;
  auto ly = iy->length;
  while (ix != rx.end() && iy != rx.end())
  {
    auto min = std::min(lx, ly);
    auto block = op(ix->data, iy->data);

    if (ix->is_fill() && iy->is_fill())
    {
      result.append(min, block != 0);
      lx -= min;
      ly -= min;
    }
    else if (ix->is_fill())
    {
      result.append_block(block);
      lx -= Bitstream::block_width;
      ly = 0;
    }
    else if (iy->is_fill())
    {
      result.append_block(block);
      ly -= Bitstream::block_width;
      lx = 0;
    }
    else
    {
      result.append_block(block, std::max(lx, ly));
      lx = ly = 0;
    }

    if (lx == 0 && ++ix != rx.end())
      lx = ix->length;

    if (ly == 0 && ++iy != ry.end())
      ly = iy->length;
  }

  if (fill_lhs)
  {
    while (ix != rx.end())
    {
      if (ix->is_fill())
        result.append(lx, ix->data);
      else
        result.append_block(ix->data, ix->length);

      if (++ix != rx.end())
        lx = ix->length;
    }
  }

  if (fill_rhs)
  {
    while (iy != ry.end())
    {
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
Bitstream and_(Bitstream const& lhs, Bitstream const& rhs)
{
  using block_type = typename Bitstream::block_type;
  return apply(lhs, rhs, false, false,
               [](block_type x, block_type y) { return x & y; });
}

template <typename Bitstream>
Bitstream or_(Bitstream const& lhs, Bitstream const& rhs)
{
  using block_type = typename Bitstream::block_type;
  return apply(lhs, rhs, true, true,
               [](block_type x, block_type y) { return x | y; });
}

template <typename Bitstream>
Bitstream xor_(Bitstream const& lhs, Bitstream const& rhs)
{
  using block_type = typename Bitstream::block_type;
  return apply(lhs, rhs, true, true,
               [](block_type x, block_type y) { return x ^ y; });
}

template <typename Bitstream>
Bitstream nand_(Bitstream const& lhs, Bitstream const& rhs)
{
  using block_type = typename Bitstream::block_type;
  return apply(lhs, rhs, true, false,
               [](block_type x, block_type y) { return x & ~y; });
}

template <typename Bitstream>
Bitstream nor_(Bitstream const& lhs, Bitstream const& rhs)
{
  using block_type = typename Bitstream::block_type;
  return apply(lhs, rhs, true, true,
               [](block_type x, block_type y) { return x | ~y; });
}

/// Transposes a vector of bitstreams into a character matrix of 0s and 1s.
/// @param out The output iterator.
/// @param v A vector of bitstreams.
template <
  typename Iterator,
  typename Bitstream,
  typename = std::enable_if_t<is_bitstream<Bitstream>::value>
>
trial<void> print(std::vector<Bitstream> const& v, Iterator&& out)
{
  if (v.empty())
    return nothing;

  using const_iterator = typename Bitstream::const_iterator;
  using ipair = std::pair<const_iterator, const_iterator>;
  std::vector<ipair> is(v.size());
  for (size_t i = 0; i < v.size(); ++i)
    is[i] = {v[i].begin(), v[i].end()};

  auto const zero_row = std::string(v.size(), '0') + '\n';
  typename Bitstream::size_type last = 0;
  bool done = false;
  while (! done)
  {
    // Compute the minimum.
    typename Bitstream::size_type min = Bitstream::npos;
    for (auto& p : is)
      if (p.first != p.second && *p.first < min)
        min = *p.first;

    if (min == Bitstream::npos)
      break;

    // Fill up the distance to the last row with 0 rows.
    auto distance = min - last;
    for (decltype(min) i = 0; i < distance; ++i)
      std::copy(zero_row.begin(), zero_row.end(), out);
    last = min + 1;

    // Print the current transposed row.
    done = true;
    for (auto& p : is)
    {
      if (p.first != p.second && *p.first == min)
      {
        *out++ = '1';
        done = false;
        ++p.first;
      }
      else
      {
        *out++ = '0';
      }
    }

    *out++ = '\n';
  }

  return nothing;
}

} // namespace vast

#endif

#ifndef VAST_BITSTREAM_H
#define VAST_BITSTREAM_H

#include <algorithm>
#include "vast/bitvector.h"
#include "vast/serialization.h"
#include "vast/traits.h"
#include "vast/util/make_unique.h"
#include "vast/util/operators.h"
#include "vast/util/print.h"
#include "vast/util/range.h"

namespace vast {

/// The base class for bit sequence ranges which provide an abstraction for
/// traversal.
template <typename Derived>
class bitstream_sequence_range
  : public util::range_facade<bitstream_sequence_range<Derived>>
{
public:
  enum block_type { fill, literal };

  // A block-based abstraction over a contiguous sequence of bits from of a
  // bitstream. A sequence can have two types: a *fill* sequence represents a
  // homogenous bits, typically greater than or equal to the block size, while
  // a *literal* sequence represents bits from a single block, typically less
  // than or equal to the block size.
  struct bitsequence
  {
    bool is_fill() const
    {
      return type == fill;
    }

    bool is_literal() const
    {
      return type == literal;
    }

    block_type type = literal;
    bitvector::size_type offset = bitvector::npos;
    bitvector::block_type data = 0;
    bitvector::size_type length = 0;
  };

protected:
  bool next()
  {
    return static_cast<Derived*>(this)->next_sequence(seq_);
  }

private:
  friend util::range_facade<bitstream_sequence_range<Derived>>;

  bitsequence const& state() const
  {
    return seq_;
  }

  bitsequence seq_;
};


/// Traits for bitstreams.
/// @note We need this mechanism because ::bitstream_base cannot access types
/// inside its CRTP template parameter.
template <typename Derived>
struct bitstream_traits;

/// The base class for all bitstream implementations.
template <typename Derived>
class bitstream_base : util::printable<bitstream_base<Derived>>
{
  friend Derived;

public:
  bitstream_base(bitstream_base const&) = default;
  bitstream_base(bitstream_base&&) = default;
  bitstream_base& operator=(bitstream_base const&) = default;
  bitstream_base& operator=(bitstream_base&&) = default;

  using size_type = bitvector::size_type;
  using block_type = bitvector::block_type;
  using const_iterator = typename bitstream_traits<Derived>::const_iterator;

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

  Derived& flip()
  {
    derived().bitwise_not();
    return static_cast<Derived&>(*this);
  }

  Derived operator~() const
  {
    Derived d(derived());
    return d.flip();
  }

  bool operator[](size_type i) const
  {
    return derived().at(i);
  }

  size_type size() const
  {
    return derived().size_impl();
  }

  bool empty() const
  {
    return derived().empty_impl();
  }

  bool append(size_type n, bool bit)
  {
    if (std::numeric_limits<size_type>::max() - n < size())
      return false;
    derived().append_impl(n, bit);
    return true;
  }

  bool push_back(bool bit)
  {
    if (std::numeric_limits<size_type>::max() == size())
      return false;
    derived().push_back_impl(bit);
    return true;
  }

  void clear() noexcept
  {
    derived().clear_impl();
  }

  const_iterator begin() const
  {
    return derived().begin_impl();
  }

  const_iterator end() const
  {
    return derived().end_impl();
  }

  size_type find_first() const
  {
    return derived().find_first_impl();
  }

  size_type find_next(size_type i) const
  {
    return derived().find_next_impl(i);
  }

  size_type find_last() const
  {
    return derived().find_last_impl();
  }

  size_type find_prev(size_type i) const
  {
    return derived().find_prev_impl(i);
  }

  bitvector const& bits() const
  {
    return derived().bits_impl();
  }

protected:
  bitstream_base() = default;

  bitstream_base(size_type n, bool bit)
  {
    derived().append_impl(n, bit);
  }

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

  template <typename Iterator>
  bool print(Iterator& out) const
  {
    // Unlike a plain bitvector, we print bitstreams from LSB to MSB.
    render(out, bits(), false, false, 0);
    return true;
  };
};

template <typename Derived>
typename bitstream_base<Derived>::size_type const bitstream_base<Derived>::npos;

namespace detail {

/// The concept for bitstream iterators.
class bitstream_iterator_concept
{
public:
  using size_type = bitvector::size_type;

  bitstream_iterator_concept() = default;
  virtual ~bitstream_iterator_concept() = default;
  virtual std::unique_ptr<bitstream_iterator_concept> copy() const = 0;

  virtual bool equals(bitstream_iterator_concept const& other) const = 0;
  virtual void increment() = 0;
  virtual size_type dereference() const = 0;
};

/// A concrete model for a specific bitstream iterator.
template <typename Iterator>
class bitstream_iterator_model : public bitstream_iterator_concept
{
public:
  bitstream_iterator_model() = default;

  bitstream_iterator_model(Iterator&& i)
    : iterator_{std::move(i)}
  {
  }

private:
  virtual std::unique_ptr<bitstream_iterator_concept> copy() const final
  {
    return make_unique<bitstream_iterator_model>(*this);
  }

  virtual bool equals(bitstream_iterator_concept const& other) const final
  {
    if (typeid(*this) != typeid(other))
      throw std::runtime_error{"incompatible iterator types"};

    return iterator_ ==
        static_cast<bitstream_iterator_model const&>(other).iterator_;
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

} // namespace detail


/// The polymorphic iterator for polymorphic bitstreams.
class bitstream_iterator
  : public util::iterator_facade<
             bitstream_iterator,
             std::forward_iterator_tag,
             bitvector::size_type,
             bitvector::size_type
          >
{
public:
  using size_type = bitvector::size_type;

  bitstream_iterator() = default;

  template <
    typename Iterator,
    typename = DisableIfSameOrDerived<bitstream_iterator, Iterator>
  >
  bitstream_iterator(Iterator&& i)
    : concept_{
        new detail::bitstream_iterator_model<Unqualified<Iterator>>{
            std::forward<Iterator>(i)}}
  {
  }

  // TODO: move implementation into *.cc file.

  bitstream_iterator(bitstream_iterator const& other)
    : concept_{other.concept_ ? other.concept_->copy() : nullptr}
  {
  }

  bitstream_iterator(bitstream_iterator&& other)
    : concept_{std::move(other.concept_)}
  {
  }

  bitstream_iterator& operator=(bitstream_iterator const& other)
  {
    concept_ = other.concept_ ? other.concept_->copy() : nullptr;
    return *this;
  }

  bitstream_iterator& operator=(bitstream_iterator&& other)
  {
    concept_ = std::move(other.concept_);
    return *this;
  }

private:
  bool equals(bitstream_iterator const& other) const
  {
    assert(concept_);
    assert(other.concept_);
    return concept_->equals(*other.concept_);
  }

  void increment()
  {
    assert(concept_);
    concept_->increment();
  }

  size_type dereference() const
  {
    assert(concept_);
    return concept_->dereference();
  }

  friend util::iterator_access;

  std::unique_ptr<detail::bitstream_iterator_concept> concept_;
};

namespace detail {

/// The concept for bitstreams.
class bitstream_concept
{
public:
  using size_type = bitvector::size_type;

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
  virtual void push_back_impl(bool bit) = 0;
  virtual void clear_impl() noexcept = 0;
  virtual bool at(size_type i) const = 0;
  virtual size_type size_impl() const = 0;
  virtual bool empty_impl() const = 0;
  virtual bitstream_iterator begin_impl() const = 0;
  virtual bitstream_iterator end_impl() const = 0;
  virtual size_type find_first_impl() const = 0;
  virtual size_type find_next_impl(size_type i) const = 0;
  virtual size_type find_last_impl() const = 0;
  virtual size_type find_prev_impl(size_type i) const = 0;
  virtual bitvector const& bits_impl() const = 0;

protected:
  bitstream_concept() = default;

private:
  friend access;
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
    return make_unique<bitstream_model>(*this);
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

  virtual void push_back_impl(bool bit) final
  {
    bitstream_.push_back_impl(bit);
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

  virtual bool empty_impl() const final
  {
    return bitstream_.empty_impl();
  }

  virtual bitstream_iterator begin_impl() const final
  {
    return bitstream_.begin_impl();
  }

  virtual bitstream_iterator end_impl() const final
  {
    return bitstream_.end_impl();
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

class bitstream;

template <>
struct bitstream_traits<bitstream>
{
  using iterator = bitstream_iterator;
  using const_iterator = iterator;
};

/// A polymorphic bitstream with value semantics.
class bitstream : public bitstream_base<bitstream>,
                  util::equality_comparable<bitstream>
{
  friend bitstream_base<bitstream>;

public:
  using const_iterator = typename bitstream_traits<bitstream>::const_iterator;

  bitstream() = default;
  bitstream(bitstream const& other);
  bitstream(bitstream&& other);

  template <
    typename Bitstream,
    typename = DisableIfSameOrDerived<bitstream, Bitstream>
  >
  bitstream(Bitstream&& bs)
    : concept_{
        new detail::bitstream_model<Unqualified<Bitstream>>{
            std::forward<Bitstream>(bs)}}
  {
  }

  bitstream& operator=(bitstream const& other);
  bitstream& operator=(bitstream&& other);

  explicit operator bool() const;

private:
  bool equals(bitstream const& other) const;
  void bitwise_not();
  void bitwise_and(bitstream const& other);
  void bitwise_or(bitstream const& other);
  void bitwise_xor(bitstream const& other);
  void bitwise_subtract(bitstream const& other);
  void append_impl(size_type n, bool bit);
  void push_back_impl(bool bit);
  void clear_impl() noexcept;
  bool at(size_type i) const;
  size_type size_impl() const;
  bool empty_impl() const;
  const_iterator begin_impl() const;
  const_iterator end_impl() const;
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
  friend bool operator==(bitstream const& x, bitstream const& y);
};

class null_bitstream;

class null_bitstream_iterator
  : public util::iterator_adaptor<
      null_bitstream_iterator,
      bitvector::one_const_iterator,
      std::forward_iterator_tag,
      bitvector::size_type,
      bitvector::size_type
    >
{
public:
  null_bitstream_iterator() = default;

  static null_bitstream_iterator begin(null_bitstream const& null);
  static null_bitstream_iterator end(null_bitstream const& null);

private:
  friend util::iterator_access;

  explicit null_bitstream_iterator(base_iterator const& i);

  auto dereference() const -> decltype(this->base().position());
};

template <>
struct bitstream_traits<null_bitstream>
{
  using iterator = null_bitstream_iterator;
  using const_iterator = iterator;
  //using seq_iterator = null_bitstream_seq_iterator;
  //using const_seq_iterator = seq_iterator;
};

/// An uncompressed bitstream that simply forwards all operations to its
/// underlying ::bitvector.
class null_bitstream : public bitstream_base<null_bitstream>,
                       util::totally_ordered<null_bitstream>
{
  template <typename>
  friend class detail::bitstream_model;
  friend bitstream_base<null_bitstream>;

public:
  using const_iterator =
    typename bitstream_traits<null_bitstream>::const_iterator;

  class sequence_range : public bitstream_sequence_range<sequence_range>
  {
  public:
    bool next_sequence(bitsequence& seq);

    explicit sequence_range(null_bitstream const& bs);
  private:
    bitvector const* bits_;
    bitvector::size_type next_ = 0;
  };

  null_bitstream() = default;
  null_bitstream(bitvector::size_type n, bool bit);

  sequence_range sequences() const;

private:
  bool equals(null_bitstream const& other) const;
  void bitwise_not();
  void bitwise_and(null_bitstream const& other);
  void bitwise_or(null_bitstream const& other);
  void bitwise_xor(null_bitstream const& other);
  void bitwise_subtract(null_bitstream const& other);
  void append_impl(size_type n, bool bit);
  void push_back_impl(bool bit);
  void clear_impl() noexcept;
  bool at(size_type i) const;
  size_type size_impl() const;
  bool empty_impl() const;
  const_iterator begin_impl() const;
  const_iterator end_impl() const;
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

  friend bool operator==(null_bitstream const& x, null_bitstream const& y);
  friend bool operator<(null_bitstream const& x, null_bitstream const& y);
};

class ewah_bitstream;

class ewah_bitstream_iterator
  : public util::iterator_facade<
             ewah_bitstream_iterator,
             std::forward_iterator_tag,
             bitvector::size_type,
             bitvector::size_type
           >
{
public:
  using size_type = bitvector::size_type;

  ewah_bitstream_iterator() = default;

  static ewah_bitstream_iterator begin(ewah_bitstream const& ewah);
  static ewah_bitstream_iterator end(ewah_bitstream const& ewah);

private:
  friend util::iterator_access;

  ewah_bitstream_iterator(ewah_bitstream const& ewah);

  bool equals(ewah_bitstream_iterator const& other) const;
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

template <>
struct bitstream_traits<ewah_bitstream>
{
  using iterator = ewah_bitstream_iterator;
  using const_iterator = iterator;
};

/// A bitstream encoded using the *Enhanced World-Aligned Hybrid (EWAH)*
/// algorithm.
///
/// @note This implementation internally maintains the following invariants:
///
///   1. The first block is a marker.
///   2. The last block is always dirty.
class ewah_bitstream : public bitstream_base<ewah_bitstream>,
                       util::totally_ordered<ewah_bitstream>,
                       util::printable<ewah_bitstream>
{
  template <typename>
  friend class detail::bitstream_model;
  friend bitstream_base<ewah_bitstream>;
  friend ewah_bitstream_iterator;

public:
  using const_iterator =
    typename bitstream_traits<ewah_bitstream>::const_iterator;

  ewah_bitstream() = default;
  ewah_bitstream(bitvector::size_type n, bool bit);

private:
  bool equals(ewah_bitstream const& other) const;
  void bitwise_not();
  void bitwise_and(ewah_bitstream const& other);
  void bitwise_or(ewah_bitstream const& other);
  void bitwise_xor(ewah_bitstream const& other);
  void bitwise_subtract(ewah_bitstream const& other);
  void append_impl(size_type n, bool bit);
  void push_back_impl(bool bit);
  void clear_impl() noexcept;
  bool at(size_type i) const;
  size_type size_impl() const;
  bool empty_impl() const;
  const_iterator begin_impl() const;
  const_iterator end_impl() const;
  size_type find_first_impl() const;
  size_type find_next_impl(size_type i) const;
  size_type find_last_impl() const;
  size_type find_prev_impl(size_type i) const;
  bitvector const& bits_impl() const;

  size_type
  find_next_from_marker(size_type bit_pos, size_type& marker_pos) const;

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

  bitvector bits_;
  size_type num_bits_ = 0;
  size_type last_marker_ = 0;

private:
  friend access;
  void serialize(serializer& sink) const;
  void deserialize(deserializer& source);

  template <typename Iterator>
  bool print(Iterator& out) const
  {
    for (size_t i = 0; i < bits_.blocks(); ++i)
    {
      if (i != bits_.blocks() - 1)
      {
        if (! bitvector::print(out, bits_.block(i)))
          return false;
        *out++ = '\n';
      }
      else
      {
        auto remaining = num_bits_ % block_width;
        if (remaining == 0)
          remaining = block_width;
        for (size_t i = 0; i < block_width - remaining; ++i)
          *out++ = ' ';
        if (! bitvector::print(out, bits_.block(i), true, 0, remaining))
          return false;
      }
    }

    return true;
  };

  friend bool operator==(ewah_bitstream const& x, ewah_bitstream const& y);
  friend bool operator<(ewah_bitstream const& x, ewah_bitstream const& y);
};


/// Transposes a vector of equal-sized bitstreams.
/// @param v A vector of bitstreams.
/// @pre All elements of *v* must have the same size.
template <
  typename Bitstream,
  typename = DisableIfSameOrDerived<Bitstream, bitstream>
>
std::vector<Bitstream> transpose(std::vector<Bitstream> const& v)
{
  if (v.empty())
    return {};
  auto vsize = v.size();
  auto bsize = v[0].size();
  if (bsize == 0)
    return {};
  for (size_t i = 0; i < vsize; ++i)
    if (v[i].size() != bsize)
      throw std::logic_error("tranpose requires same-size bitstreams");

  std::vector<typename Bitstream::size_type> next(vsize);
  auto min = Bitstream::npos;
  for (size_t i = 0; i < vsize; ++i)
  {
    next[i] = v[i].find_first();
    if (next[i] < min)
      min = next[i];
  }
  auto all_zero = min;
  std::vector<Bitstream> result;
  while (result.size() != bsize)
  {
    assert(min != Bitstream::npos);
    if (all_zero > 0)
      result.resize(result.size() + all_zero, {vsize, false});
    result.emplace_back(Bitstream());
    auto& row = result.back();
    for (size_t i = 0; i < vsize; ++i)
      row.push_back(next[i] == min);
    for (size_t i = 0; i < vsize; ++i)
    {
      if (next[i] != Bitstream::npos && next[i] == min)
        next[i] = v[i].find_next(next[i]);
    }
    auto new_min = std::min_element(next.begin(), next.end());
    all_zero = *new_min - min - 1;
    min = *new_min;
  }
  return result;
}

} // namespace vast

#endif

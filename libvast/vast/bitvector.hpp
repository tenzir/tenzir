#ifndef VAST_BITVECTOR_HPP
#define VAST_BITVECTOR_HPP

#include <cstdint>
#include <limits>
#include <string>
#include <vector>
#include <type_traits>

#include "vast/bits.hpp"
#include "vast/detail/assert.hpp"
#include "vast/detail/operators.hpp"
#include "vast/detail/iterator.hpp"
#include "vast/detail/range.hpp"
#include "vast/word.hpp"

namespace vast {
namespace detail {

template <typename Bitvector>
class bitvector_iterator;

template <class Block>
class bitvector_range_iterator;

} // namespace detail

/// A vector of bits as in `std::vector<bool>`, except that the underlying
/// block/word type is configurable. This implementation describes a super set
/// of the interface defined in §23.3.12.
template <class Block = size_t, class Allocator = std::allocator<Block>>
class bitvector : detail::equality_comparable<bitvector<Block, Allocator>> {
  static_assert(std::is_unsigned<Block>::value,
                "Block must be unsigned for well-defined bit operations");
  static_assert(!std::is_same<Block, bool>::value,
                "Block cannot be bool; you may want std::vector<bool> instead");

public:
  using value_type = bool;
  using allocator_type = Allocator;
  using size_type = size_t;
  class reference;
  using const_reference = bool;
  using pointer = reference*;
  using const_pointer = bool const*;
  using iterator = detail::bitvector_iterator<bitvector>;
  using const_iterator = detail::bitvector_iterator<bitvector const>;
  using reverse_iterator = std::reverse_iterator<iterator>;
  using const_reverse_iterator = std::reverse_iterator<const_iterator>;

  // -- construct/destruct/assign ---------------------------------------------

  bitvector();
  explicit bitvector(const Allocator& alloc);
  explicit bitvector(size_type n, const Allocator& alloc = Allocator{});
  bitvector(size_type n, const bool& value, const Allocator& = Allocator());

  template <class InputIterator>
  bitvector(InputIterator first, InputIterator last,
            const Allocator& = Allocator());

  bitvector(const bitvector& x) = default;
  bitvector(bitvector&& x) = default;

  bitvector(const bitvector&, const Allocator&);
  bitvector(bitvector&&, const Allocator&);

  bitvector(std::initializer_list<value_type>, const Allocator& = Allocator());

  ~bitvector() = default;

  bitvector& operator=(bitvector const&) = default;
  bitvector& operator=(bitvector&&) = default;
  bitvector& operator=(std::initializer_list<value_type>);

  template <class InputIterator>
  void assign(InputIterator first, InputIterator last);
  void assign(size_type n, const value_type& t);
  void assign(std::initializer_list<value_type>);

  allocator_type get_allocator() const noexcept;

  // -- iterators -------------------------------------------------------------

  iterator begin() noexcept;
  const_iterator begin() const noexcept;
  iterator end() noexcept;
  const_iterator end() const noexcept;
  reverse_iterator rbegin() noexcept;
  const_reverse_iterator rbegin() const noexcept;
  reverse_iterator rend() noexcept;
  const_reverse_iterator rend() const noexcept;

  const_iterator cbegin() const noexcept;
  const_iterator cend() const noexcept;
  const_reverse_iterator crbegin() const noexcept;
  const_reverse_iterator crend() const noexcept;

  // -- capacity --------------------------------------------------------------

  bool empty() const noexcept;
  size_type size() const noexcept;
  size_type max_size() const noexcept;
  size_type capacity() const noexcept;
  void resize(size_type n, value_type value = false);
  void reserve(size_type n);
  void shrink_to_fit();

  // -- element access --------------------------------------------------------

  reference operator[](size_type i);
  const_reference operator[](size_type i) const;
  const_reference at(size_type n) const;
  reference at(size_type n);
  reference front();
  const_reference front() const;
  reference back();
  const_reference back() const;

  // -- modifiers -------------------------------------------------------------

  template <class... Ts>
  void emplace_back(Ts&&... xs);

  void push_back(const value_type& x);

  void pop_back();

  // TODO: provide implementation as needed
  //template <class... Ts>
  //iterator emplace(const_iterator i, Ts&&... xs);
  //iterator insert(const_iterator i, const value_type& x);
  //iterator insert(const_iterator i, size_type n, const value_type& x);
  //template <class InputIterator>
  //iterator insert(const_iterator i, InputIterator first, InputIterator last);
  //iterator insert(const_iterator i, std::initializer_list<value_type> list);
  //iterator erase(const_iterator i);
  //iterator erase(const_iterator first, const_iterator last);

  void swap(bitvector& other);

  void flip() noexcept;

  void clear() noexcept;

  // -- relational operators --------------------------------------------------

  template <class B, class A>
  friend bool operator==(bitvector<B, A> const& x, bitvector<B, A> const& y);

  template <class B, class A>
  friend bool operator<(bitvector<B, A> const& x, bitvector<B, A> const& y);

  // -------------------------------------------------------------------------
  // -- non-standard extensions ----------------------------------------------
  // -------------------------------------------------------------------------

  template <class>
  friend class detail::bitvector_range_iterator; // bit_range(..)

  // The functionality below enhances the stock version of std::vector<bool>.

  using block = Block;
  using word = word<Block>;
  static constexpr auto npos = word::npos;

  class bits_iterator;

  /// Counts the number of 1-bits in the bit vector.
  /// Also known as *population count* or *Hamming weight*.
  /// @returns The number of bits set to 1.
  size_type count() const noexcept;

  /// Appends a single block or a prefix of a block.
  /// @param x The block value.
  /// @param n The number of bits of *x* to append, counting from the LSB.
  /// @pre `bits > 0 && bits <= word::width`
  void append_block(block x, size_type bits = word::width);

  /// Appends a sequence of blocks.
  /// @param first An iterator to the first complete block to append.
  /// @param last An iterator one past the end of the last block.
  template <class InputIterator>
  void append_blocks(InputIterator first, InputIterator last);

  // -- concepts --------------------------------------------------------------

  template <class Inspector>
  friend auto inspect(Inspector& f, bitvector& b) {
    return f(b.blocks_, b.size_);
  }

private:
  static size_type bits_to_blocks(size_type n) {
    return 1 + ((n - 1) / word::width);
  }

  block& block_at_bit(size_type i) {
    return blocks_[i / word::width];
  }

  const block& block_at_bit(size_type i) const {
    return blocks_[i / word::width];
  }

  size_type partial_bits() const {
    return size_ % word::width;
  }

  std::vector<block> blocks_;
  size_type size_;
};

template <class Block, class Allocator>
class bitvector<Block, Allocator>::reference {
  friend class bitvector<Block, Allocator>;

public:
  operator bool() const noexcept {
    return (*block_ & mask_) != 0;
  }

  bool operator~() const noexcept {
    return (*block_ & mask_) == 0;
  }

  reference& operator=(bool x) noexcept {
    x ? *block_ |= mask_ : *block_ &= ~mask_;
    return *this;
  }

  reference& operator=(reference const& other) noexcept {
    other ? *block_ |= mask_ : *block_ &= ~mask_;
  }

  void flip() noexcept {
    *block_ ^= mask_;
  }

  friend void swap(reference x, reference y) noexcept {
    bool b = x;
    x = y;
    y = b;
  }

private:
  // The standard defines it, but why do we need it?
  //reference() noexcept;

  reference(block* x, block mask) : block_{x}, mask_{mask} {
  }

  typename bitvector<Block, Allocator>::block* block_;
  typename bitvector<Block, Allocator>::block const mask_;
};

// -- construct/move/assign --------------------------------------------------

template <class Block, class Allocator>
bitvector<Block, Allocator>::bitvector() : bitvector{Allocator{}} {
  // nop
}

template <class Block, class Allocator>
bitvector<Block, Allocator>::bitvector(const Allocator& alloc)
  : blocks_{alloc},
    size_{0} {
  // nop
}

template <class Block, class Allocator>
bitvector<Block, Allocator>::bitvector(size_type n, const Allocator& alloc)
  : blocks_(bits_to_blocks(n), 0, alloc),
    size_{n} {
  // nop
}

template <class Block, class Allocator>
bitvector<Block, Allocator>::bitvector(size_type n, const value_type& value,
                                       const Allocator& alloc)
  : blocks_(bits_to_blocks(n), value ? word::all : word::none, alloc),
    size_{n} {
}

template <class Block, class Allocator>
template <class InputIterator>
bitvector<Block, Allocator>::bitvector(InputIterator first, InputIterator last,
                                       const Allocator& alloc)
  : bitvector{alloc},
    size_{0} {
  assign(first, last);
}

template <class Block, class Allocator>
bitvector<Block, Allocator>::bitvector(const bitvector& other,
                                       const Allocator& alloc)
  : blocks_{other.blocks_, alloc},
    size_{other.size_} {
  // nop
}

template <class Block, class Allocator>
bitvector<Block, Allocator>::bitvector(bitvector&& other,
                                       const Allocator& alloc)
  : blocks_{std::move(other.blocks_), alloc},
    size_{std::move(other.size_)} {
  // nop
}

template <class Block, class Allocator>
bitvector<Block, Allocator>::bitvector(std::initializer_list<value_type> list,
                                       const Allocator& alloc)
  : blocks_{alloc},
    size_{0} {
  assign(list.begin(), list.end());
}

template <class Block, class Allocator>
template <class InputIterator>
void bitvector<Block, Allocator>::assign(InputIterator first,
                                         InputIterator last) {
  blocks_.clear();
  while (first != last)
    push_back(*first++);
}

namespace detail {

template <typename Bitvector>
class bitvector_iterator
  : public detail::iterator_facade<
      bitvector_iterator<Bitvector>,
      typename Bitvector::value_type,
      std::random_access_iterator_tag,
      std::conditional_t<
        std::is_const<Bitvector>::value,
        typename Bitvector::const_reference,
        typename Bitvector::reference
      >,
      typename Bitvector::size_type
    > {
  friend Bitvector;
public:
  bitvector_iterator() = default;

private:
  friend detail::iterator_access;

  bitvector_iterator(Bitvector& bv, typename Bitvector::size_type off = 0)
    : bitvector_{&bv}, i_{off} {
  }

  bool equals(bitvector_iterator const& other) const {
    return i_ == other.i_;
  }

  void increment() {
    VAST_ASSERT(i_ != Bitvector::npos);
    ++i_;
  }

  void decrement() {
    VAST_ASSERT(i_ != Bitvector::npos);
    --i_;
  }

  void advance(typename Bitvector::size_type n) {
    i_ += n;
  }

  auto dereference() const {
    VAST_ASSERT(!bitvector_->empty());
    VAST_ASSERT(i_ < bitvector_->size());
    return (*bitvector_)[i_];
  }

  Bitvector* bitvector_ = nullptr;
  typename Bitvector::size_type i_ = Bitvector::npos;
};

} // namespace detail

// -- iterators -------------------------------------------------------------

template <class Block, class Allocator>
typename bitvector<Block, Allocator>::iterator
bitvector<Block, Allocator>::begin() noexcept {
  return {*this};
}

template <class Block, class Allocator>
typename bitvector<Block, Allocator>::const_iterator
bitvector<Block, Allocator>::begin() const noexcept {
  return {*this};
}

template <class Block, class Allocator>
typename bitvector<Block, Allocator>::iterator
bitvector<Block, Allocator>::end() noexcept {
  return {*this, size()};
}

template <class Block, class Allocator>
typename bitvector<Block, Allocator>::const_iterator
bitvector<Block, Allocator>::end() const noexcept {
  return {*this, size()};
}

template <class Block, class Allocator>
typename bitvector<Block, Allocator>::reverse_iterator
bitvector<Block, Allocator>::rbegin() noexcept {
  return typename bitvector<Block, Allocator>::reverse_iterator{end()};
}

template <class Block, class Allocator>
typename bitvector<Block, Allocator>::const_reverse_iterator
bitvector<Block, Allocator>::rbegin() const noexcept {
  return typename bitvector<Block, Allocator>::const_reverse_iterator{end()};
}

template <class Block, class Allocator>
typename bitvector<Block, Allocator>::reverse_iterator
bitvector<Block, Allocator>::rend() noexcept {
  return typename bitvector<Block, Allocator>::reverse_iterator{begin()};
}

template <class Block, class Allocator>
typename bitvector<Block, Allocator>::const_reverse_iterator
bitvector<Block, Allocator>::rend() const noexcept {
  return typename bitvector<Block, Allocator>::const_reverse_iterator{begin()};
}

template <class Block, class Allocator>
typename bitvector<Block, Allocator>::const_iterator
bitvector<Block, Allocator>::cbegin() const noexcept {
  return begin();
}

template <class Block, class Allocator>
typename bitvector<Block, Allocator>::const_iterator
bitvector<Block, Allocator>::cend() const noexcept {
  return end();
}

template <class Block, class Allocator>
typename bitvector<Block, Allocator>::const_reverse_iterator
bitvector<Block, Allocator>::crbegin() const noexcept {
  return rbegin();
}

template <class Block, class Allocator>
typename bitvector<Block, Allocator>::const_reverse_iterator
bitvector<Block, Allocator>::crend() const noexcept {
  return rend();
}

// -- capacity --------------------------------------------------------------

template <class Block, class Allocator>
bool bitvector<Block, Allocator>::empty() const noexcept {
  return size_ == 0;
}

template <class Block, class Allocator>
typename bitvector<Block, Allocator>::size_type
bitvector<Block, Allocator>::size() const noexcept {
  return size_;
}

template <class Block, class Allocator>
typename bitvector<Block, Allocator>::size_type
bitvector<Block, Allocator>::max_size() const noexcept {
  return blocks_.max_size() * word::width;
}

template <class Block, class Allocator>
typename bitvector<Block, Allocator>::size_type
bitvector<Block, Allocator>::capacity() const noexcept {
  auto c = blocks_.capacity() * word::width;
  auto p = partial_bits();
  return p == 0 ? c : c + word::width - p;
}

template <class Block, class Allocator>
void bitvector<Block, Allocator>::resize(size_type n, value_type value) {
  if (size_ >= n) {
    blocks_.resize(bits_to_blocks(n));
    size_ = n;
    return;
  }
  // Fill up last word first.
  auto p = partial_bits();
  if (p > 0) {
    auto m = word::all << p;
    value ? blocks_.back() |= m : blocks_.back() &= ~m;
    // If everything fits in the last word, we're done.
    if (n - size_ <= word::width - p) {
      size_ = n;
      return;
    }
  }
  // Fill remaining words.
  blocks_.resize(bits_to_blocks(n), value ? word::all : word::none);
  size_ = n;
}

template <class Block, class Allocator>
void bitvector<Block, Allocator>::reserve(size_type n) {
  blocks_.reserve(bits_to_blocks(n));
}

template <class Block, class Allocator>
void bitvector<Block, Allocator>::shrink_to_fit() {
  blocks_.shrink_to_fit();
}

// -- element access ----------------------------------------------------------

template <class Block, class Allocator>
typename bitvector<Block, Allocator>::reference
bitvector<Block, Allocator>::operator[](size_type i) {
  VAST_ASSERT(i < size_);
  return {&block_at_bit(i), word::mask(i % word::width)};
}

template <class Block, class Allocator>
typename bitvector<Block, Allocator>::const_reference
bitvector<Block, Allocator>::operator[](size_type i) const {
  VAST_ASSERT(i < size_);
  return (block_at_bit(i) & word::mask(i % word::width)) != 0;
}

template <class Block, class Allocator>
typename bitvector<Block, Allocator>::reference
bitvector<Block, Allocator>::at(size_type i) {
  if (i >= size_)
    throw std::out_of_range("bitvector");
  return (*this)[i];
}

template <class Block, class Allocator>
typename bitvector<Block, Allocator>::const_reference
bitvector<Block, Allocator>::at(size_type i) const {
  if (i >= size_)
    throw std::out_of_range("bitvector");
  return (*this)[i];
}

template <class Block, class Allocator>
typename bitvector<Block, Allocator>::reference
bitvector<Block, Allocator>::front() {
  VAST_ASSERT(!empty());
  return (*this)[0];
}

template <class Block, class Allocator>
typename bitvector<Block, Allocator>::const_reference
bitvector<Block, Allocator>::front() const {
  VAST_ASSERT(!empty());
  return (*this)[0];
}

template <class Block, class Allocator>
typename bitvector<Block, Allocator>::reference
bitvector<Block, Allocator>::back() {
  VAST_ASSERT(!empty());
  return (*this)[size_ - 1];
}

template <class Block, class Allocator>
typename bitvector<Block, Allocator>::const_reference
bitvector<Block, Allocator>::back() const {
  VAST_ASSERT(!empty());
  return (*this)[size_ - 1];
}

// -- modifiers ---------------------------------------------------------------

template <class Block, class Allocator>
template <class... Ts>
void bitvector<Block, Allocator>::emplace_back(Ts&&... xs) {
  return push_back(std::forward<Ts>(xs)...);
}

/// Appends a single bit to the end of the bit vector.
/// @param bit The value of the bit.
template <class Block, class Allocator>
void bitvector<Block, Allocator>::push_back(const value_type& x) {
  auto p = partial_bits();
  if (p == 0)
    blocks_.push_back(x ? word::all : word::none);
  else if (x)
    blocks_.back() |= word::all << p;
  else
    blocks_.back() &= ~(word::all << p);
  ++size_;
}

template <class Block, class Allocator>
void bitvector<Block, Allocator>::pop_back() {
  VAST_ASSERT(!empty());
  if (partial_bits() == 1)
    blocks_.pop_back();
  --size_;
}

template <class Block, class Allocator>
void bitvector<Block, Allocator>::swap(bitvector& other) {
  using std::swap;
  swap(blocks_, other.blocks_);
  swap(size_, other.size_);
}

template <class Block, class Allocator>
void bitvector<Block, Allocator>::flip() noexcept {
  for (auto& block : blocks_)
    block = ~block;
}

template <class Block, class Allocator>
void bitvector<Block, Allocator>::clear() noexcept {
  blocks_.clear();
  size_ = 0;
}

template <class Block, class Allocator>
bool operator==(bitvector<Block, Allocator> const& x,
                bitvector<Block, Allocator> const& y) {
  if (x.size_ != y.size_)
    return false;
  // Compare all but last block.
  auto xbegin = x.blocks_.begin();
  auto xend = x.blocks_.end();
  auto ybegin = y.blocks_.begin();
  auto yend = y.blocks_.end();
  if (xbegin == xend)
    return true;
  --xend;
  --yend;
  if (!std::equal(xbegin, xend, ybegin, yend))
    return false;
  // Compare last block.
  using word = typename bitvector<Block, Allocator>::word;
  auto xlast = *xend & word::lsb_mask(x.partial_bits());
  auto ylast = *yend & word::lsb_mask(y.partial_bits());
  return xlast == ylast;
}

template <class Block, class Allocator>
typename bitvector<Block, Allocator>::size_type
bitvector<Block, Allocator>::count() const noexcept {
  auto n = size_;
  auto p = blocks_.data();
  auto cnt = size_type{0};
  for (; n >= word::width; ++p, n -= word::width)
    cnt += word::popcount(*p);
  if (n > 0)
    cnt += word::popcount(*p & word::lsb_mask(n));
  return cnt;
}

template <class Block, class Allocator>
void bitvector<Block, Allocator>::append_block(block x, size_type bits) {
  VAST_ASSERT(bits > 0);
  VAST_ASSERT(bits <= word::width);
  auto p = partial_bits();
  if (p == 0) {
    blocks_.push_back(x);
  } else {
    auto& last = blocks_.back();
    last = (last & word::lsb_mask(p)) | (x << p);
    auto available = word::width - p;
    if (bits > available)
      blocks_.push_back(x >> available);
  }
  size_ += bits;
}

template <class Block, class Allocator>
template <class InputIterator>
void bitvector<Block, Allocator>::append_blocks(InputIterator first,
                                                InputIterator last) {
  auto p = partial_bits();
  if (p == 0) {
    blocks_.insert(blocks_.end(), first, last);
    size_ = blocks_.size() * word::width;
  } else {
    while (first != last) {
      auto x = *first;
      auto& last = blocks_.back();
      last = (last & word::lsb_mask(p)) | (x << p);
      blocks_.push_back(x >> (word::width - p));
      size_ += word::width;
      ++first;
    }
  }
}

namespace detail {

template <class Block>
class bitvector_range_iterator
  : public iterator_facade<
      bitvector_range_iterator<Block>,
      bits<Block> const,
      std::forward_iterator_tag,
      bits<Block> const&
    > {
public:
  static bitvector_range_iterator begin(bitvector<Block> const& v) {
    auto i = bitvector_range_iterator{v, begin_tag{}};
    if (!v.empty())
      i.scan();
    return i;
  }

  static bitvector_range_iterator end(bitvector<Block> const& v) {
    return {v, end_tag{}};
  }

private:
  struct begin_tag {};
  struct end_tag {};

  bitvector_range_iterator(bitvector<Block> const& v, begin_tag)
    : bitvector_{&v},
      block_{v.blocks_.begin()} {
  }

  bitvector_range_iterator(bitvector<Block> const& v, end_tag)
    : bitvector_{&v},
      block_{v.blocks_.end()} {
  }

  friend iterator_access;

  // Finds the next 1-bit.
  void scan() {
    VAST_ASSERT(block_ != bitvector_->blocks_.end());
    auto last = bitvector_->blocks_.end() - 1;
    if (block_ == last) {
      // Process the last block.
      auto p = bitvector_->partial_bits();
      bits_ = {*block_, p == 0 ? word<Block>::width : p};
    } else if (!word<Block>::all_or_none(*block_)) {
      // Process an inhomogeneous block.
      bits_ = {*block_, word<Block>::width};
    } else {
      // Scan for consecutive runs of all-0 or all-1 blocks.
      auto n = word<Block>::width;
      auto data = *block_;
      ++block_;
      while (block_ != last && *block_ == data) {
        n += word<Block>::width;
        ++block_;
      }
      bits_ = {data, n};
    }
  }

  bool equals(bitvector_range_iterator const& other) const {
    return block_ == other.block_;
  }

  void increment() {
    scan();
    ++block_;
  }

  bits<Block> const& dereference() const {
    return bits_;
  }

  bitvector<Block> const* bitvector_;
  typename std::vector<Block>::const_iterator block_;
  bits<Block> bits_;
};

} // namespace detail

template <class Block, class Allocator>
auto bit_range(bitvector<Block, Allocator> const& v) {
  return detail::make_iterator_range(
    detail::bitvector_range_iterator<Block>::begin(v),
    detail::bitvector_range_iterator<Block>::end(v));
}

} // namespace vast

#endif

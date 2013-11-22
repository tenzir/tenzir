#include "vast/bitvector.h"

#include "vast/serialization.h"

namespace vast {

using size_type = bitvector::size_type;
using block_type = bitvector::block_type;

constexpr bitvector::block_type bitvector::all_zero;
constexpr bitvector::block_type bitvector::all_one;
constexpr bitvector::block_type bitvector::msb_one;
constexpr bitvector::size_type bitvector::block_width;
constexpr bitvector::size_type bitvector::npos;

namespace {

uint8_t count_table[] =
{
  0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4, 1, 2, 2, 3, 2, 3, 3, 4, 2,
  3, 3, 4, 3, 4, 4, 5, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3,
  3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3,
  4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 2, 3, 3, 4,
  3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5,
  6, 6, 7, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4,
  4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5,
  6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 2, 3, 3, 4, 3, 4, 4, 5,
  3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 3,
  4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7, 4, 5, 5, 6, 5, 6, 6, 7, 5, 6,
  6, 7, 6, 7, 7, 8
};

} // namespace <anonymous>

bitvector::reference::reference(block_type& block, block_type i)
  : block_(block)
  , mask_(block_type{1} << i)
{
  assert(i < block_width);
}

bitvector::reference& bitvector::reference::flip()
{
  block_ ^= mask_;
  return *this;
}

bitvector::reference::operator bool() const
{
  return (block_ & mask_) != 0;
}

bool bitvector::reference::operator~() const
{
  return (block_ & mask_) == 0;
}

bitvector::reference& bitvector::reference::operator=(bool x)
{
  x ? block_ |= mask_ : block_ &= ~mask_;
  return *this;
}

bitvector::reference& bitvector::reference::operator=(reference const& other)
{
  other ? block_ |= mask_ : block_ &= ~mask_;
  return *this;
}

bitvector::reference& bitvector::reference::operator|=(bool x)
{
  if (x)
    block_ |= mask_;
  return *this;
}

bitvector::reference& bitvector::reference::operator&=(bool x)
{
  if (! x)
    block_ &= ~mask_;
  return *this;
}

bitvector::reference& bitvector::reference::operator^=(bool x)
{
  if (x)
    block_ ^= mask_;
  return *this;
}

bitvector::reference& bitvector::reference::operator-=(bool x)
{
  if (x)
    block_ &= ~mask_;
  return *this;
}

size_type bitvector::count(block_type block)
{
  size_type n = 0;
  while (block)
  {
    // TODO: use __popcnt if available.
    n += count_table[block & ((1u << 8) - 1)];
    block >>= 8;
  }
  return n;
}

size_type bitvector::lowest_bit(block_type block)
{
  auto x = block - (block & (block - 1)); // Extract right-most 1-bit.
  size_type log = 0;
  while (x >>= 1)
    ++log;
  return log;
}

size_type bitvector::highest_bit(block_type block)
{
  size_type log = 0;
  while (block >>= 1)
    ++log;
  return log;
}

size_type bitvector::next_bit(block_type block, size_type i)
{
  if (i >= block_width - 1)
    return npos;
  auto masked = block & (all_one << ++i);
  return masked ? lowest_bit(masked) : npos;
}

size_type bitvector::prev_bit(block_type block, size_type i)
{
  if (i == 0)
    return npos;
  if (i > block_width)
    i = block_width;
  auto masked = block & ~(all_one << (i % block_width));
  return masked ? highest_bit(masked) : npos;
}

bitvector::bitvector()
  : num_bits_{0}
{
}

bitvector::bitvector(size_type size, bool value)
  : bits_(bits_to_blocks(size), value ? all_one : 0),
    num_bits_{size}
{
}

bitvector bitvector::operator~() const
{
  bitvector b(*this);
  b.flip();
  return b;
}

bitvector bitvector::operator<<(size_type n) const
{
  bitvector b(*this);
  return b <<= n;
}

bitvector bitvector::operator>>(size_type n) const
{
  bitvector b(*this);
  return b >>= n;
}

bitvector& bitvector::operator<<=(size_type n)
{
  if (n >= num_bits_)
    return reset();

  if (n > 0)
  {
    auto last = blocks() - 1;
    auto div = n / block_width;
    auto r = bit_index(n);
    auto b = &bits_[0];
    assert(blocks() >= 1);
    assert(div <= last);

    if (r != 0)
    {
      for (size_type i = last - div; i > 0; --i)
        b[i + div] = (b[i] << r) | (b[i - 1] >> (block_width - r));
      b[div] = b[0] << r;
    }
    else
    {
      for (size_type i = last-div; i > 0; --i)
        b[i + div] = b[i];
      b[div] = b[0];
    }

    std::fill_n(b, div, all_zero);
    zero_unused_bits();
  }

  return *this;
}

bitvector& bitvector::operator>>=(size_type n)
{
  if (n >= num_bits_)
      return reset();

  if (n > 0)
  {
    auto last = blocks() - 1;
    auto div = n / block_width;
    auto r = bit_index(n);
    auto b = &bits_[0];
    assert(blocks() >= 1);
    assert(div <= last);

    if (r != 0)
    {
      for (size_type i = last - div; i > 0; --i)
        b[i - div] = (b[i] >> r) | (b[i + 1] << (block_width - r));
      b[last - div] = b[last] >> r;
    }
    else
    {
      for (size_type i = div; i <= last; ++i)
        b[i-div] = b[i];
    }

    std::fill_n(b + (blocks() - div), div, all_zero);
  }

  return *this;
}

bitvector& bitvector::operator&=(bitvector const& other)
{
  assert(size() >= other.size());
  for (size_type i = 0; i < other.blocks(); ++i)
    bits_[i] &= other.bits_[i];
  return *this;
}

bitvector& bitvector::operator|=(bitvector const& other)
{
  assert(size() >= other.size());
  for (size_type i = 0; i < other.blocks(); ++i)
    bits_[i] |= other.bits_[i];
  return *this;
}

bitvector& bitvector::operator^=(bitvector const& other)
{
  assert(size() >= other.size());
  for (size_type i = 0; i < other.blocks(); ++i)
    bits_[i] ^= other.bits_[i];
  return *this;
}

bitvector& bitvector::operator-=(bitvector const& other)
{
  assert(size() >= other.size());
  for (size_type i = 0; i < other.blocks(); ++i)
    bits_[i] &= ~other.bits_[i];
  return *this;
}

bitvector operator&(bitvector const& x, bitvector const& y)
{
  bitvector b(x);
  return b &= y;
}

bitvector operator|(bitvector const& x, bitvector const& y)
{
  bitvector b(x);
  return b |= y;
}

bitvector operator^(bitvector const& x, bitvector const& y)
{
  bitvector b(x);
  return b ^= y;
}

bitvector operator-(bitvector const& x, bitvector const& y)
{
  bitvector b(x);
  return b -= y;
}

void bitvector::resize(size_type n, bool value)
{
  auto old = blocks();
  auto required = bits_to_blocks(n);
  auto block_value = value ? all_one : all_zero;

  if (required != old)
    bits_.resize(required, block_value);

  if (value && n > num_bits_ && extra_bits())
    bits_[old - 1] |= (block_value << extra_bits());

  num_bits_ = n;
  zero_unused_bits();
}

void bitvector::clear() noexcept
{
  bits_.clear();
  num_bits_ = 0;
}

void bitvector::push_back(bool bit)
{
  auto s = size();
  resize(s + 1);
  set(s, bit);
}

void bitvector::append(block_type block, size_type bits)
{
  assert(bits <= block_width);
  auto used = extra_bits();
  auto unused = block_width - used;
  auto masked_block = bits == block_width ? block : block & ~(all_one << bits);
  if (used == 0)
  {
    bits_.push_back(masked_block);
  }
  else
  {
    bits_.back() |= masked_block << used;
    if (bits > unused)
      bits_.push_back(masked_block >> unused);
  }
  num_bits_ += bits;
}

bitvector& bitvector::set(size_type i, bool bit)
{
  assert(i < num_bits_);

  if (bit)
      bits_[block_index(i)] |= bit_mask(i);
  else
      reset(i);

  return *this;
}

bitvector& bitvector::set()
{
  std::fill(bits_.begin(), bits_.end(), all_one);
  zero_unused_bits();
  return *this;
}

bitvector& bitvector::reset(size_type i)
{
  assert(i < num_bits_);
  bits_[block_index(i)] &= ~bit_mask(i);
  return *this;
}

bitvector& bitvector::reset()
{
  std::fill(bits_.begin(), bits_.end(), all_zero);
  return *this;
}

bitvector& bitvector::flip(size_type i)
{
  assert(i < num_bits_);
  bits_[block_index(i)] ^= bit_mask(i);
  return *this;
}

bitvector& bitvector::flip()
{
  for (size_type i = 0; i < blocks(); ++i)
      bits_[i] = ~bits_[i];
  zero_unused_bits();
  return *this;
}

bitvector::const_reference bitvector::operator[](size_type i) const
{
  assert(i < num_bits_);
  return (bits_[block_index(i)] & bit_mask(i)) != 0;
}

bitvector::reference bitvector::operator[](size_type i)
{
  assert(i < num_bits_);
  return {bits_[block_index(i)], bit_index(i)};
}

block_type bitvector::block(size_type b) const
{
  return bits_[b];
}

block_type& bitvector::block(size_type b)
{
  return bits_[b];
}

block_type bitvector::block_at_bit(size_type i) const
{
  return bits_[block_index(i)];
}

block_type& bitvector::block_at_bit(size_type i)
{
  return bits_[block_index(i)];
}

block_type bitvector::first_block() const
{
  assert(! bits_.empty());
  return bits_.front();
}

block_type& bitvector::first_block()
{
  assert(! bits_.empty());
  return bits_.front();
}

block_type bitvector::last_block() const
{
  assert(! bits_.empty());
  return bits_.back();
}

block_type& bitvector::last_block()
{
  assert(! bits_.empty());
  return bits_.back();
}

size_type bitvector::count() const
{
  auto i = bits_.begin();
  size_type n = 0;
  auto length = blocks();
  while (length)
  {
    n += count(*i);
    ++i;
    --length;
  }
  return n;
}

size_type bitvector::blocks() const
{
  return bits_.size();
}

size_type bitvector::size() const
{
  return num_bits_;
}

bool bitvector::empty() const
{
  return bits_.empty();
}

block_type bitvector::extra_bits() const
{
  return bit_index(size());
}

size_type bitvector::find_first() const
{
  return find_forward(0);
}

size_type bitvector::find_next(size_type i) const
{
  if (size() == 0 || i >= size() - 1)
    return npos;
  auto bi = block_index(++i);
  auto block = bits_[bi] & (all_one << bit_index(i));
  return block ? bi * block_width + lowest_bit(block) : find_forward(bi + 1);
}

size_type bitvector::find_last() const
{
  return size() == 0 ? npos : find_backward(blocks() - 1);
}

size_type bitvector::find_prev(size_type i) const
{
  if (i >= (size() - 1) || size() == 0)
    return npos;
  auto bi = block_index(--i);
  auto block = bits_[bi] & ~(all_one << (bit_index(i) + 1));
  if (block)
    return bi * block_width + highest_bit(block);
  else if (bi > 0)
    return find_backward(bi - 1);
  else
    return npos;
}

void bitvector::zero_unused_bits()
{
  if (extra_bits())
    bits_.back() &= ~(all_one << extra_bits());
}

size_type bitvector::find_forward(size_type i) const
{
  while (i < blocks() && bits_[i] == 0)
    ++i;
  if (i >= blocks())
    return npos;
  return i * block_width + lowest_bit(bits_[i]);
}

size_type bitvector::find_backward(size_type i) const
{
  if (i >= blocks())
    return npos;
  while (i > 0 && bits_[i] == 0)
    --i;
  auto result = i * block_width + highest_bit(bits_[i]);
  return result == 0 ? npos : result;
}

void bitvector::serialize(serializer& sink) const
{
  sink << num_bits_;
  sink << bits_;
}

void bitvector::deserialize(deserializer& source)
{
  source >> num_bits_;
  source >> bits_;
}

bool operator==(bitvector const& x, bitvector const& y)
{
  return x.num_bits_ == y.num_bits_ && x.bits_ == y.bits_;
}

bool operator<(bitvector const& x, bitvector const& y)
{
  assert(x.size() == y.size());
  for (size_type r = x.blocks(); r > 0; --r)
  {
    auto i = r - 1;
    if (x.bits_[i] < y.bits_[i])
      return true;
    else if (x.bits_[i] > y.bits_[i])
      return false;
  }
  return false;
}

} // namespace vast

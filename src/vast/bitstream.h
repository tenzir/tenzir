#ifndef VAST_BITSTREAM_H
#define VAST_BITSTREAM_H

#include <algorithm>
#include "vast/serialization.h"
#include "vast/bitvector.h"
#include "vast/exception.h"

namespace vast {

/// An append-only sequence of bits.
template <typename Derived>
class bitstream_base
{
public:
  typedef bitvector::size_type size_type;
  static size_type constexpr npos = bitvector::npos;

  friend bool operator==(Derived const& x, Derived const& y)
  {
    return x.equals(y);
  }

  friend bool operator!=(Derived const& x, Derived const& y)
  {
    return ! (x == y);
  }

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

  bitvector const& bits() const
  {
    return bits_;
  }

  size_type find_first() const
  {
    return derived().find_first_impl();
  }

  size_type find_next(size_type i) const
  {
    return derived().find_next_impl(i);
  }

protected:
  bitstream_base() = default;

  bitstream_base(size_type n, bool bit)
  {
    derived().append_impl(n, bit);
  }

  bitvector bits_;

private:
  friend access;

  void serialize(serializer& sink)
  {
    sink << bits_;
  }

  void deserialize(deserializer& source)
  {
    source >> bits_;
  }

  Derived& derived()
  {
    return *static_cast<Derived*>(this);
  }

  Derived const& derived() const
  {
    return *static_cast<Derived const*>(this);
  }
};

/// Converts a bitstream to a `std::string`. Unlike a plain bitvector, we
/// print bitstreams from LSB to MSB.
///
/// @param bs The bitstream to convert.
///
/// @return A `std::string` representation of *bs*.
template <typename Derived>
std::string to_string(bitstream_base<Derived> const& bs)
{
  return to_string(bs.bits(), false, false, 0);
}

/// An uncompressed bitstream that simply forwards all operations to the
/// underlying ::bitvector.
class null_bitstream : public bitstream_base<null_bitstream>
{
  typedef bitstream_base<null_bitstream> super;
  friend super;

public:
  null_bitstream() = default;
  null_bitstream(bitvector::size_type n, bool bit)
    : super(n, bit)
  {
  }

private:
  void bitwise_not();
  void bitwise_and(null_bitstream const& other);
  void bitwise_or(null_bitstream const& other);
  void bitwise_xor(null_bitstream const& other);
  void bitwise_subtract(null_bitstream const& other);
  bool equals(null_bitstream const& other) const;
  void append_impl(size_type n, bool bit);
  void push_back_impl(bool bit);
  void clear_impl() noexcept;
  bool at(size_type i) const;
  size_type size_impl() const;
  size_type empty_impl() const;
  size_type find_first_impl() const;
  size_type find_next_impl(size_type i) const;
};

/// Transposes a vector of equal-sized bitstreams.
/// @param v A vector of bitstreams.
/// @pre All elements of *v* must have the same size.
template <typename Bitstream>
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
      throw exception("tranpose requires same-size bitstreams");

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

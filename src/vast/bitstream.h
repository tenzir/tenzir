#ifndef VAST_BITSTREAM_H
#define VAST_BITSTREAM_H

#include "vast/bitvector.h"

namespace vast {

/// An append-only sequence of bits.
template <typename Derived>
class bitstream
{
public:
  typedef bitvector::size_type size_type;
  static size_type constexpr npos = bitvector::npos;

  bitstream() = default;

  bitstream(size_type n, bool bit)
  {
    derived().append_impl(n, bit);
  }

  friend bool operator==(Derived const& x, Derived const& y)
  {
    return x.equals(y);
  }

  friend bool operator!=(Derived const& x, Derived const& y)
  {
    return ! (x == y);
  }

  friend Derived operator&(Derived const& x, Derived const& y)
  {
    Derived d(x);
    return d &= y;
  }

  friend Derived operator|(Derived const& x, Derived const& y)
  {
    Derived d(x);
    return d |= y;
  }

  friend Derived operator^(Derived const& x, Derived const& y)
  {
    Derived d(x);
    return d ^= y;
  }

  friend Derived operator-(Derived const& x, Derived const& y)
  {
    Derived d(x);
    return d -= y;
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

  void append(size_type n, bool bit)
  {
    derived().append_impl(n, bit);
  }

  void push_back(bool bit)
  {
    derived().push_back_impl(bit);
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
  bitvector bits_;

private:
  Derived& derived()
  {
    return *static_cast<Derived*>(this);
  }

  Derived const& derived() const
  {
    return *static_cast<Derived const*>(this);
  }

  template <typename Archive>
  friend void serialize(Archive& oa, bitstream const& bs)
  {
    oa << bs.bits_;
  }

  template <typename Archive>
  friend void deserialize(Archive& ia, bitstream& bs)
  {
    ia >> bs.bits_;
  }
};

class null_bitstream : public bitstream<null_bitstream>
{
  typedef bitstream<null_bitstream> super;
  friend super;

public:
  null_bitstream() = default;
  null_bitstream(bitvector::size_type n, bool bit)
    : super(n, bit)
  {
  }

  null_bitstream& operator&=(null_bitstream const& other);
  null_bitstream& operator|=(null_bitstream const& other);
  null_bitstream& operator^=(null_bitstream const& other);
  null_bitstream& operator-=(null_bitstream const& other);
  null_bitstream& flip();

private:
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

} // namespace vast

#endif

#ifndef VAST_BITSTREAM_H
#define VAST_BITSTREAM_H

#include "vast/bitvector.h"

namespace vast {

/// An append-only sequence of bits.
template <typename Derived>
class bitstream
{
public:
  bitstream() = default;

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

  bool operator[](size_t i) const
  {
    return derived().at(i);
  }

  size_t size() const
  {
    return derived().size_impl();
  }

  bool empty() const
  {
    return derived().empty_impl();
  }

  void append(size_t n, bool bit)
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

  size_t find_first() const
  {
    return derived().find_first_impl();
  }

  size_t find_next(size_t i) const
  {
    return derived().find_next_impl(i);
  }

protected:
  bitstream(bitvector::size_type n, bool bit)
    : bits_(n, bit)
  {
  }

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
  void append_impl(size_t n, bool bit);
  void push_back_impl(bool bit);
  void clear_impl() noexcept;
  bool at(size_t i) const;
  size_t size_impl() const;
  size_t empty_impl() const;
  size_t find_first_impl() const;
  size_t find_next_impl(size_t i) const;
};

} // namespace vast

#endif

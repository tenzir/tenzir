#ifndef VAST_BITSTREAM_H
#define VAST_BITSTREAM_H

#include "vast/bitvector.h"

namespace vast {

/// An append-only sequence of bits.
template <typename Derived>
class bitstream
{
  bitstream() = delete;

public:
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
    derived().flip();
  }

  void append(size_t n, bool bit)
  {
    derived().append_impl(n, bit);
  }

  void push_back(bool bit)
  {
    derived().push_back_impl(bit);
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
};

class null_bitstream : public bitstream<null_bitstream>
{
  friend class bitstream<null_bitstream>;

public:
  null_bitstream& operator&=(null_bitstream const& other);
  null_bitstream& operator|=(null_bitstream const& other);
  null_bitstream& operator^=(null_bitstream const& other);
  null_bitstream& operator-=(null_bitstream const& other);
  void flip();

private:
  bool equals(null_bitstream const& other) const;
  void append_impl(size_t n, bool bit);
  void push_back_impl(bool bit);
};

} // namespace vast

#endif

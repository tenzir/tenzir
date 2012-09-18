#ifndef VAST_BITSTREAM_H
#define VAST_BITSTREAM_H

#include "vast/bitvector.h"

namespace vast {

/// An append-only sequence of bits.
class bitstream
{
public:
  virtual ~bitstream() = default;
  virtual void append(size_t n, bool bit) = 0;
  virtual void push_back(bool bit) = 0;
  virtual void flip() = 0;
  bitvector const& bits() const;

protected:
  bitvector bits_;
};

/// An operator mixin for concrete bit streams.
template <typename Derived>
class basic_bitstream : public bitstream
{
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

  virtual void append(size_t n, bool bit) = 0;
  virtual void push_back(bool bit) = 0;
  virtual void flip() = 0;

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

class null_bitstream : public basic_bitstream<null_bitstream>
{
  friend class basic_bitstream<null_bitstream>;

public:
  null_bitstream& operator&=(null_bitstream const& other);
  null_bitstream& operator|=(null_bitstream const& other);
  null_bitstream& operator^=(null_bitstream const& other);
  null_bitstream& operator-=(null_bitstream const& other);
  virtual void append(size_t n, bool bit);
  virtual void push_back(bool bit);
  virtual void flip();

private:
  bool equals(null_bitstream const& other) const;
};

} // namespace vast

#endif

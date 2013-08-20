#ifndef VAST_BITSTREAM_H
#define VAST_BITSTREAM_H

#include <algorithm>
#include "vast/serialization.h"
#include "vast/bitvector.h"
#include "vast/exception.h"
#include "vast/util/make_unique.h"

namespace vast {
namespace detail {

// The runtime concept for bitstreams.
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
  virtual size_type find_first_impl() const = 0;
  virtual size_type find_next_impl(size_type i) const = 0;
  virtual bitvector const& bits_impl() const = 0;
};

// A concrete bitstream concept.
template <typename Bitstream>
class bitstream_model : public bitstream_concept
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

public:
  bitstream_model(Bitstream const& x)
    : bitstream_{x}
  {
  }

  bitstream_model(Bitstream&& bs)
    : bitstream_{std::move(bs)}
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

  virtual size_type find_first_impl() const final
  {
    return bitstream_.find_first_impl();
  }

  virtual size_type find_next_impl(size_type i) const final
  {
    return bitstream_.find_next_impl(i);
  }

  virtual bitvector const& bits_impl() const final
  {
    return bitstream_.bits_impl();
  }

private:
  Bitstream bitstream_;
};

} // namespace detail

/// The base class for concrete bitstream implementations.
template <typename Derived>
class bitstream_base
{
public:
  using size_type = detail::bitstream_concept::size_type;
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
    return derived().bits_impl();
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

  Derived& derived()
  {
    return *static_cast<Derived*>(this);
  }

  Derived const& derived() const
  {
    return *static_cast<Derived const*>(this);
  }
};

/// An append-only sequence of bits.
class bitstream : public bitstream_base<bitstream>
{
  friend bitstream_base<bitstream>;

public:
  bitstream() = default;
  bitstream(bitstream const& other);
  bitstream(bitstream&& other);

  template <typename Bitstream>
  bitstream(Bitstream&& bs)
    : concept_{make_unique<detail::bitstream_model<Bitstream>>(std::move(bs))}
  {
  }

  bitstream& operator=(bitstream const&) = default;
  bitstream& operator=(bitstream&&) = default;

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
  size_type find_first_impl() const;
  size_type find_next_impl(size_type i) const;
  bitvector const& bits_impl() const;

  std::unique_ptr<detail::bitstream_concept> concept_;
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
  friend detail::bitstream_model<null_bitstream>;
  friend bitstream_base<null_bitstream>;

public:
  null_bitstream() = default;
  null_bitstream(bitvector::size_type n, bool bit)
    : bits_(n, bit)
  {
  }

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
  size_type find_first_impl() const;
  size_type find_next_impl(size_type i) const;
  bitvector const& bits_impl() const;

  bitvector bits_;
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

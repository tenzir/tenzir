#ifndef VAST_BITMAP_H
#define VAST_BITMAP_H

#include <list>
#include <stdexcept>
#include <unordered_map>
#include "vast/bitstream.h"
#include "vast/operator.h"
#include "vast/serialization/all.h"
#include "vast/util/operators.h"

namespace vast {
namespace detail {

/// Takes any arithmetic type and creates and maps it to an unsigned integral
/// type whose values have a bitwise total ordering. That is, it contructs an
/// *offset binary* encoding, which bitmap range coders rely upon.
template <
  typename T,
  typename = std::enable_if_t<
    std::is_unsigned<T>::value && std::is_integral<T>::value
  >
>
T order(T x)
{
  // Unsigned integral types already exhibit a bitwise total order.
  return x;
}

template <
  typename T,
  typename = std::enable_if_t<
    std::is_signed<T>::value && std::is_integral<T>::value
  >
>
auto order(T x) -> std::make_unsigned_t<T>
{
  // For signed integral types, We shift the entire domain by 2^w to the left,
  // where w is the size of T in bits. By ditching 2's-complete, we get a total
  // bitwise ordering.
  x += std::make_unsigned_t<T>{1} << std::numeric_limits<T>::digits;
  return static_cast<std::make_unsigned_t<T>>(x);
}

template <
  typename T,
  typename = std::enable_if_t<std::is_floating_point<T>::value>
>
uint64_t order(T x, size_t sig_bits = 5)
{
  static_assert(std::numeric_limits<T>::is_iec559,
                "can only order IEEE 754 double types");

  assert(sig_bits >= 0 && sig_bits <= 52);

  static constexpr auto exp_mask = (~0ull << 53) >> 1;
  static constexpr auto sig_mask = ~0ull >> 12;

  auto p = reinterpret_cast<uint64_t*>(&x);
  bool positive = ~*p & (1ull << 63);
  auto exp = (*p & exp_mask) >> 52;
  auto sig = *p & sig_mask;

  // If the value is positive we add a 1 as MSB left of the exponent and if
  // the value is negative, we make the MSB 0. If the value is negative we
  // also have to reverse the exponent to ensure that, e.g., -1 is considered
  // *smaller* than -0.1, although the exponent of -1 is *larger* than -0.1.
  // Because the exponent already has a offset-binary encoding, this merely
  // involves subtracting it from 2^11-1.
  auto result = positive ? exp | (1ull << 12) : (exp_mask >> 52) - exp;

  // Next, we add the desired bits of the significand. Because the
  // significand is always great or equal to 0, we can use the same
  // subtraction method for negative values as for the offset-binary encoded
  // exponent.
  if (sig_bits > 0)
  {
    result <<= sig_bits;
    result |= (positive ? sig : sig_mask - sig) >> (52 - sig_bits);
  }

  return result;
}

/// Decomposes a single value into a vector of values according to the given
/// base.
/// @param x The value to decompose.
/// @param base The base used to decompose *x*.
/// @param values The values in which *x* gets decomposed.
/// @pre `base.size() == values.size()`
template <typename T, typename U = T>
void decompose(T x, std::vector<U> const& base, std::vector<U>& values)
{
  assert(base.size() == values.size());
  auto o = order(x);
  size_t i = base.size();
  while (i --> 0)
  {
    auto b = U{1};
    for (size_t j = 0; j < i; ++j)
      b *= base[j];

    values[i] = o / b;

    if (o >= b)
      o -= values[i] * b;
  }
}

} // namespace detail

/// The base class for bitmap coders. A coder offers encoding and decoding of
/// values. It encodes values into its type-specific storage and returns a
/// bitstream as a result of a point-query under a given relational operator.
template <typename Derived>
class coder
  : util::equality_comparable<coder<Derived>>,
    util::orable<coder<Derived>>
{
public:
  uint64_t size() const
  {
    return rows_;
  }

  // Encodes a single values multiple times.
  // @param x The value to encode.
  // @param n The number of time to add *x*.
  /// @returns `true` on success.
  template <typename T>
  bool encode(T x, size_t n = 1)
  {
    if (std::numeric_limits<uint64_t>::max() - rows_ < n)
      return false;
    if (! derived()->encode_impl(x, n))
      return false;
    rows_ += n;
    return true;
  }

  /// Aritifically increases the size, i.e., the number of rows.
  /// @param n The number of rows to increase the coder by.
  /// @returns `true` on success and `false` if there is not enough space.
  bool stretch(size_t n)
  {
    if (std::numeric_limits<uint64_t>::max() - rows_ < n)
      return false;
    rows_ += n;
    return true;
  }

  /// Decodes a value under a relational operator.
  /// @param x The value to decode.
  /// @param op The relation operator under which to decode *x*.
  /// @returns The bitstream for *x* according to *op*.
  template <typename T, typename Hack = Derived>
  auto decode(T x, relational_operator op = equal) const
    -> decltype(std::declval<Hack>().decode_impl(x, op))
  {
    return derived()->decode_impl(x, op);
  }

  Derived& operator|=(Derived const& other)
  {
    derived()->bitwise_or(other);
    if (other.size() > size())
      rows_ = other.size();
    return *derived();
  }

  /// Applies a function to each raw bitstream.
  template <typename F>
  void each(F f) const
  {
    derived()->each_impl(f);
  }

protected:
  friend access;

  void serialize(serializer& sink) const
  {
    sink << static_cast<uint64_t>(rows_);
  }

  void deserialize(deserializer& source)
  {
    source >> rows_;
  }

  friend bool operator==(coder const& x, coder const& y)
  {
    return x.rows_ == y.rows_;
  }

private:
  Derived* derived()
  {
    return static_cast<Derived*>(this);
  }

  Derived const* derived() const
  {
    return static_cast<Derived const*>(this);
  }

  uint64_t rows_ = 0;
};

/// An equality bitmap coder. It uses a hash table to provide a one-to-one
/// mapping of values to bitstreams. Consequently, it occupies *O(c)* space
/// where *c* is the attribute cardinality. Because range queries of the form
/// *x < v*, where *v* is a fixed value, would run in time *O(v)*, we only
/// support equality and inequality operations with this coder.
template <typename T, typename Bitstream>
class equality_coder
  : public coder<equality_coder<T, Bitstream>>,
    util::equality_comparable<equality_coder<T, Bitstream>>
{
  using super = coder<equality_coder<T, Bitstream>>;
  friend super;

private:
  bool encode_impl(T x, size_t n)
  {
    auto i = bitstreams_.find(x);
    if (i != bitstreams_.end())
    {
      auto& bs = i->second;
      bs.append(this->size() - bs.size(), false);
      return bs.append(n, true);
    }
    else
    {
      Bitstream bs{this->size(), false};
      return bs.append(n, true) && bitstreams_.emplace(x, std::move(bs)).second;
    }
  }

  trial<Bitstream> decode_impl(T x, relational_operator op) const
  {
    if (! (op == equal || op == not_equal))
      return error{"unsupported relational operator:", op};

    auto i = bitstreams_.find(x);
    if (i == bitstreams_.end() || i->second.empty())
      return Bitstream{this->size(), op == not_equal};

    auto result = i->second;
    result.append(this->size() - result.size(), false);

    return std::move(op == equal ? result : result.flip());
  }

  template <typename F>
  void each_impl(F f) const
  {
    for (auto& p : bitstreams_)
      f(1, p.first, p.second);
  }

  void bitwise_or(equality_coder const& other)
  {
    if (other.bitstreams_.empty())
      return;

    if (bitstreams_.empty())
    {
      bitstreams_ = other.bitstreams_;
      return;
    }

    for (auto& p : bitstreams_)
    {
      auto i = other.bitstreams_.find(p.first);
      if (i != other.bitstreams_.end())
        p.second |= i->second;
      else if (this->size() < other.size())
        p.second.append(other.size() - this->size(), false);
    }

    for (auto& p : other.bitstreams_)
      if (bitstreams_.find(p.first) == bitstreams_.end())
      {
        auto j = bitstreams_.insert(p).first;
        if (this->size() > other.size())
          j->second.append(other.size() - this->size(), false);
      }
  }

  std::unordered_map<T, Bitstream> bitstreams_;

private:
  friend access;

  void serialize(serializer& sink) const
  {
    super::serialize(sink);
    sink << bitstreams_;
  }

  void deserialize(deserializer& source)
  {
    super::deserialize(source);
    source >> bitstreams_;
  }

  friend bool operator==(equality_coder const& x, equality_coder const& y)
  {
    return static_cast<super const&>(x) == static_cast<super const&>(y)
        && x.bitstreams_ == y.bitstreams_;
  }
};

/// An binary bit-slice coder (aka. *binary bit-sliced index*). It uses
/// *O(log(b))* space, with *b* being the number of bits needed to represent
/// the given type `T`. Adding a value *x* decomposes it into it's individual
/// bits and records each bit value in a separate bitstream. For example, the
/// value 4, would entail appending a 1 to the bitstream for 2^2 and a 0 to to
/// all other bitstreams.
template <typename T, typename Bitstream>
class binary_bitslice_coder
  : public coder<binary_bitslice_coder<T, Bitstream>>,
    util::equality_comparable<binary_bitslice_coder<T, Bitstream>>
{
  static_assert(std::is_integral<T>::value,
                "binary encoding requires an integral type");

  using super = coder<binary_bitslice_coder<T, Bitstream>>;
  friend super;

  static uint8_t constexpr bits = std::numeric_limits<T>::digits;

public:
  binary_bitslice_coder()
    : bitstreams_(bits)
  {
  }

  /// Retrieves a bitstream for a given power of 2.
  /// @param mag The order of magnitude.
  /// @pre `mag < bits` where *bits* represents the number of bits in `T`.
  Bitstream const& get(size_t mag) const
  {
    return bitstreams_[mag];
  }

private:
  bool encode_impl(T x, size_t n)
  {
    for (size_t i = 0; i < bits; ++i)
    {
      bitstreams_[i].append(this->size() - bitstreams_[i].size(), false);
      if (! bitstreams_[i].append(n, (x >> i) & 1))
        return false;
    }
    return true;
  }

  trial<Bitstream> decode_impl(T x, relational_operator op) const
  {
    switch (op)
    {
      default:
        return error{"unsupported relational operator: ", op};
      case equal:
      case not_equal:
        {
          Bitstream r{this->size(), true};
          for (size_t i = 0; i < bits; ++i)
            r &= ((x >> i) & 1) ? bitstreams_[i] : ~bitstreams_[i];

          return {std::move(op == equal ? r : r.flip())};
        }
    }
  }

  template <typename F>
  void each_impl(F f) const
  {
    for (size_t i = 0; i < bits; ++i)
      f(1, i, bitstreams_[i]);
  }

  void bitwise_or(binary_bitslice_coder const& other)
  {
    if (bitstreams_.empty())
      bitstreams_ = other.bitstreams_;
    else if (! other.bitstreams_.empty())
      for (size_t i = 0; i < bits; ++i)
        bitstreams_[i] |= other.bitstreams_[i];
  }

  std::vector<Bitstream> bitstreams_;

private:
  friend access;

  void serialize(serializer& sink) const
  {
    super::serialize(sink);
    sink << bitstreams_;
  }

  void deserialize(deserializer& source)
  {
    super::deserialize(source);
    source >> bitstreams_;
  }

  friend bool operator==(binary_bitslice_coder const& x,
                         binary_bitslice_coder const& y)
  {
    return static_cast<super const&>(x) == static_cast<super const&>(y)
        && x.bitstreams_ == y.bitstreams_;
  }
};

/// The base class for multi-component bit-slice coders. Given a *base* it
/// decomposes a value into a *value list*. For example, the value list 846 for
/// a 3-component base of <10,10,10> would be <8, 4, 6>.
template <typename Derived, typename T, typename Bitstream>
class bitslice_coder
  : public coder<bitslice_coder<Derived, T, Bitstream>>,
    util::equality_comparable<bitslice_coder<Derived, T, Bitstream>>
{
  static_assert(std::is_arithmetic<T>::value,
                "bitslice coding requires an arithmetic type");

  using super = coder<bitslice_coder<Derived, T, Bitstream>>;
  friend super;

public:
  using offset_binary_type = decltype(detail::order(T()));
  using value_list = std::vector<offset_binary_type>;

  /// Constructs a slicer with a given (potentially non-uniform) base.
  /// @param base The sequence of bases.
  /// @pre `! base.empty()` and `b >= 2` for all *b* in *base*.
  explicit bitslice_coder(value_list base)
    : base_{std::move(base)},
      v_(base_.size()),
      bitstreams_(base_.size())
  {
    assert(! base_.empty());
    assert(std::all_of(base_.begin(),
                       base_.end(),
                       [](size_t b) { return b >= 2; }));

    initialize();
  }

  /// Constructs a bit-slice coder with a *uniform* base.
  /// @param base The base for all components.
  /// @param n The number of components.
  /// @pre `base >= 2 && n > 0`
  bitslice_coder(offset_binary_type base, size_t n)
    : base_(n, base),
      v_(base_.size()),
      bitstreams_(base_.size())
  {
    assert(base >= 2);
    assert(n > 0);

    initialize();
  }


protected:
  value_list base_;
  value_list v_;
  std::vector<std::vector<Bitstream>> bitstreams_;

private:
  void initialize()
  {
    for (size_t i = 0; i < bitstreams_.size(); ++i)
      bitstreams_[i].resize(base_[i]);

    // Any base b requires only b-1 bitstreams because one can obtain any
    // bitstream through conjunction/disjunction of the others. While this
    // decreases space requirements by 1/b, it increases query time by b-1.
    // Only for the special case of b == 2 we use 1 bitstream because it
    // does not impair query performance.
    //for (size_t i = 0; i < bitstreams_.size(); ++i)
    //  if (base_[i] == 2)
    //    bitstreams_[i].resize(1);
  }

private:
  bool encode_impl(T x, size_t n)
  {
    return static_cast<Derived*>(this)->encode_value(x, n);
  }

  trial<Bitstream> decode_impl(T x, relational_operator op) const
  {
    return static_cast<Derived const*>(this)->decode_value(x, op);
  }

  template <typename F>
  void each_impl(F f) const
  {
    assert(base_.size() == bitstreams_.size());
    for (size_t i = 0; i < bitstreams_.size(); ++i)
      for (size_t j = 0; j < bitstreams_[i].size(); ++j)
        f(i, j, bitstreams_[i][j]);
  }

  void bitwise_or(bitslice_coder const& other)
  {
    // With some effort it is possible to OR together two bitslice coders of
    // different bases, but it requires conversion of the base. We tackle this
    // maybe in the future
    assert(base_ == other.base_);
    assert(! bitstreams_.empty());
    assert(! other.bitstreams_.empty());
    assert(bitstreams_.size() == other.bitstreams_.size());

    if (bitstreams_[0].empty())
      bitstreams_ = other.bitstreams_;
    else if (! other.bitstreams_[0].empty())
      for (size_t i = 0; i < bitstreams_.size(); ++i)
        for (size_t j = 0; j < bitstreams_[i].size(); ++j)
          bitstreams_[i][j] |= other.bitstreams_[i][j];
  }

private:
  friend access;

  void serialize(serializer& sink) const
  {
    super::serialize(sink);
    sink << base_ << v_ << bitstreams_;
  }

  void deserialize(deserializer& source)
  {
    super::deserialize(source);
    source >> base_ >> v_ >> bitstreams_;
  }

  friend bool operator==(bitslice_coder const& x, bitslice_coder const& y)
  {
    return static_cast<super const&>(x) == static_cast<super const&>(y)
        && x.base_ == y.base_
        && x.bitstreams_ == y.bitstreams_;
  }
};

/// An equality bit-slice coder.
template <typename T, typename Bitstream>
class equality_bitslice_coder
  : public bitslice_coder<
      equality_bitslice_coder<T, Bitstream>, T, Bitstream
    >
{
  using super = bitslice_coder<
      equality_bitslice_coder<T, Bitstream>, T, Bitstream
    >;

  friend super;
  using super::v_;
  using super::base_;
  using super::bitstreams_;

public:
  using super::super;

  equality_bitslice_coder()
    : super{10, std::numeric_limits<T>::digits10 + 1}
  {
  }

private:
  bool encode_value(T x, size_t n)
  {
    detail::decompose(x, base_, v_);
    for (size_t i = 0; i < v_.size(); ++i)
      if (v_[i] > 0)
      {
        auto idx = base_[i] == 2 ? 0 : v_[i];
        auto& bs = bitstreams_[i][idx];
        bs.append(this->size() - bs.size(), false);
        if (! bs.append(n, true))
          return false;
      }

    return true;
  }

  trial<Bitstream> decode_value(T x, relational_operator op) const
  {
    detail::decompose(x, base_, const_cast<decltype(v_)&>(v_));

    Bitstream r{this->size(), true};
    for (size_t i = 0; i < v_.size(); ++i)
    {
      auto idx = v_[i];
      if (base_[i] == 2 && idx != 0)
        --idx;
      r &= bitstreams_[i][idx];
    }

    switch (op)
    {
      default:
        return error{"unsupported relational operator: ", op};
      case equal:
        return std::move(r);
      case not_equal:
        return std::move(~r);
    }
  }
};

/// A range bit-slice coder.
/// To evaluate equality and range predicates, it uses the *RangeEval-Opt*
/// algorithm by Chee-Yong Chan and Yannis E. Ioannidis.
template <typename T, typename Bitstream>
class range_bitslice_coder
  : public bitslice_coder<
      range_bitslice_coder<T, Bitstream>, T, Bitstream
    >
{
  using super = bitslice_coder<
      range_bitslice_coder<T, Bitstream>, T, Bitstream
    >;

  friend super;
  using super::v_;
  using super::base_;
  using super::bitstreams_;

public:
  range_bitslice_coder()
    : super{10, std::numeric_limits<T>::digits10 + 1}
  {
    for (auto& component : bitstreams_)
      component.resize(component.size() - 1);
  }

private:
  bool encode_value(T x, size_t n)
  {
    detail::decompose(x, base_, v_);
    for (size_t i = 0; i < bitstreams_.size(); ++i)
      for (size_t j = 0; j < bitstreams_[i].size(); ++j)
      {
        auto& bs = bitstreams_[i][j];
        bs.append(this->size() - bs.size(), false);
        if (! bs.append(n, j >= v_[i]))
          return false;
      }
    return true;
  }

  // Implements the *RangeEval-Opt* algorithm.
  // TODO: add some optimizations from Ming-Chuan Wu to reduce the number of
  // bitstream scans (and bitwise operations).
  trial<Bitstream> decode_value(T x, relational_operator op) const
  {
    if (x == std::numeric_limits<T>::min())
    {
      if (op == less)  // A < min => false
        return Bitstream{this->size(), false};
      else if (op == greater_equal) // A >= min => true
        return Bitstream{this->size(), true};
    }
    else if (op == less || op == greater_equal)
    {
      --x;
    }

    Bitstream result{this->size(), true};

    detail::decompose(x, base_, const_cast<decltype(v_)&>(v_));

    switch (op)
    {
      default:
        return error{"unsupported relational operator: ", op};
      case less:
      case less_equal:
      case greater:
      case greater_equal:
        {
          if (v_[0] < base_[0] - 1) // && bitstream != all_ones
            result = bitstreams_[0][v_[0]];

          for (size_t i = 1; i < v_.size(); ++i)
          {
            if (v_[i] != base_[i] - 1) // && bitstream != all_ones
              result &= bitstreams_[i][v_[i]];

            if (v_[i] != 0) // && bitstream != all_ones
              result |= bitstreams_[i][v_[i] - 1];
          }
        }
        break;
      case equal:
      case not_equal:
        {
          for (size_t i = 0; i < v_.size(); ++i)
          {
            if (v_[i] == 0) // && bitstream != all_ones
              result &= bitstreams_[i][0];
            else if (v_[i] == base_[i] - 1)
              result &= ~bitstreams_[i][base_[i] - 2];
            else
              result &= bitstreams_[i][v_[i]] ^ bitstreams_[i][v_[i] - 1];
          }
        }
        break;
    }

    if (op == greater || op == greater_equal || op == not_equal)
      return std::move(~result);
    else
      return std::move(result);
  }
};

/// A null binning policy acting as identity function.
template <typename T>
struct null_binner : util::equality_comparable<null_binner<T>>
{
  T operator()(T x) const
  {
    return x;
  }

  void serialize(serializer&) const
  {
  }

  void deserialize(deserializer&)
  {
  }

  friend bool operator==(null_binner const&, null_binner const&)
  {
    return true;
  }
};

/// A binning policy that reduces value to a given precision.
template <typename T>
class precision_binner
{
  template <typename B>
  using is_bool = std::is_same<B, bool>;

  template <typename F>
  using is_double = std::is_same<F, double>;

  static_assert(std::is_arithmetic<T>::value && ! is_bool<T>::value,
      "precision binning works only with number types");

  constexpr static int default_precision =
    std::conditional_t<is_double<T>::value,
      std::integral_constant<int, -2>,
      std::integral_constant<int, 1>
    >::value;

public:
  /// Constructs a precision binner.
  ///
  /// @param precision The number of decimal digits. For example, a value of 3
  /// means that the values 1000 and 1300 end up in the same bin having a value
  /// of 1.
  ///
  /// For integral types, the sign of *precision* has no meaning, but for
  /// floating point types, the sign indiciates the precision of the fractional
  /// component. For example, a precision of -2 means that the values 42.03 and
  /// 42.04 end up in the same bin 42.00.
  ///
  /// @note Integral types are truncated and fractional types are rounded.
  precision_binner(int precision = default_precision)
  {
    integral_ = std::pow(10, precision < 0 ? -precision : precision);
    if (precision < 0)
      fractional_ = integral_;
  }

  T operator()(T x) const
  {
    return dispatch(x, is_double<T>());
  }

private:
  T dispatch(T x, std::true_type) const
  {
    if (fractional_ != 0.0)
    {
      double i;
      auto f = std::modf(x, &i);
      return i + std::round(f * fractional_) / fractional_;
    }
    else if (integral_)
    {
      return std::round(x / integral_);
    }
    return x;
  }

  T dispatch(T x, std::false_type) const
  {
    return x / integral_;
  }

  T integral_;
  double fractional_ = 0.0;

private:
  friend access;

  void serialize(serializer& sink) const
  {
    sink << integral_ << fractional_;
  }

  void deserialize(deserializer& source)
  {
    source >> integral_ >> fractional_;
  }

  friend bool operator==(precision_binner const& x, precision_binner const& y)
  {
    return x.integral_ == y.integral_ && x.fractional_ == y.fractional_;
  }
};

/// A bitmap acts as an associative array mapping arithmetic values to
/// [bitstreams](@ref bitstream).
template <
  typename T,
  typename Bitstream = ewah_bitstream,
  template <typename, typename> class Coder = equality_coder,
  template <typename> class Binner = null_binner
>
class bitmap
  : util::equality_comparable<bitmap<T, Bitstream, Coder, Binner>>,
    util::orable<bitmap<T, Bitstream, Coder, Binner>>
{
  static_assert(std::is_arithmetic<T>::value, "arithmetic type required");

public:
  using data_type = T;
  using bitstream_type = Bitstream;
  using binner_type = Binner<T>;
  using coder_type = Coder<T, Bitstream>;

  /// Default-constructs an empty bitmap.
  bitmap() = default;

  bitmap& operator|=(bitmap const& other)
  {
    coder_ |= other.coder_;
    return *this;
  }

  /// Instantites a new binner.
  /// @param xs The parameters forwarded to the constructor of the binner.
  template <typename... Ts>
  void binner(Ts&&... xs)
  {
    binner_ = binner_type(std::forward<Ts>(xs)...);
  }

  /// Adds a value to the bitmap. For example, in the case of equality
  /// encoding, this entails appending 1 to the single bitstream for the given
  /// value and 0 to all other bitstreams.
  /// @param x The value to append.
  /// @param n The number of times to append *x*.
  /// @returns `true` on success and `false` if the bitmap is full, i.e., has
  ///          `2^std::numeric_limits<size_t>::digits() - 1` elements.
  bool push_back(T x, size_t n = 1)
  {
    return coder_.encode(binner_(x), n);
  }

  /// Aritifically increases the bitmap size, i.e., the number of rows.
  /// @param n The number of rows to increase the bitmap by.
  /// @returns `true` on success and `false` if there is not enough space.
  bool stretch(size_t n)
  {
    return coder_.stretch(n);
  }

  /// Shorthand for `lookup(equal, x)`.
  trial<Bitstream> operator[](T x) const
  {
    return lookup(equal, x);
  }

  /// Retrieves a bitstream of a given value with respect to a given operator.
  ///
  /// @param op The relational operator to use for looking up *x*.
  ///
  /// @param x The value to find the bitstream for.
  ///
  /// @returns An engaged bitstream for all values *v* where *op(v,x)* is
  /// `true` or a disengaged bitstream if *x* does not exist.
  trial<Bitstream> lookup(relational_operator op, T x) const
  {
    return coder_.decode(binner_(x), op);
  }

  /// Retrieves the bitmap size.
  /// @returns The number of elements contained in the bitmap.
  uint64_t size() const
  {
    return coder_.size();
  }

  /// Checks whether the bitmap is empty.
  /// @returns `true` *iff* the bitmap has 0 entries.
  bool empty() const
  {
    return size() == 0;
  }

  /// Accesses the underlying coder of the bitmap.
  /// @returns The coder of this bitmap.
  coder_type const& coder() const
  {
    return coder_;
  }

private:
  coder_type coder_;
  binner_type binner_;

private:
  friend access;

  void serialize(serializer& sink) const
  {
    sink << binner_ << coder_;
  }

  void deserialize(deserializer& source)
  {
    source >> binner_ >> coder_;
  }

  template <typename Iterator>
  friend trial<void> print(bitmap const& bm, Iterator&& out,
                           bool with_header = true, char delim = '\t')
  {
    if (bm.empty())
      return nothing;

    std::string str;
    if (with_header)
    {
      bm.coder_.each(
          [&](size_t, T x, Bitstream const&) { str += to_string(x) + delim; });

      str.pop_back();
      str += '\n';
    }

    print(str, out);

    std::vector<Bitstream> cols;
    bm.coder_.each([&](size_t, T, Bitstream const& bs) { cols.push_back(bs); });

    return print(cols, out);
  }

  friend bool operator==(bitmap const& x, bitmap const& y)
  {
    return x.coder_ == y.coder_ && x.binner_ == y.binner_;
  }
};

/// A bitmap specialization for `bool`.
template <
  typename Bitstream,
  template <typename, typename> class Coder,
  template <typename> class Binner
>
class bitmap<bool, Bitstream, Coder, Binner>
{
public:
  using binner_type = Binner<bool>;
  using coder_type = Coder<bool, Bitstream>;

  bitmap() = default;

  bool push_back(bool x, size_t n = 1)
  {
    return bool_.append(n, x);
  }

  bool stretch(size_t n)
  {
    return bool_.append(n, false);
  }

  trial<Bitstream> operator[](bool x) const
  {
    return lookup(x);
  }

  trial<Bitstream> lookup(relational_operator op, bool x) const
  {
    switch (op)
    {
      default:
        return error{"unsupported relational operator: ", op};
      case not_equal:
        return {x ? ~bool_ : bool_};
      case equal:
        return {x ? bool_ : ~bool_};
    }
  }

  uint64_t size() const
  {
    return bool_.size();
  }

  bool empty() const
  {
    return bool_.empty();
  }

private:
  Bitstream bool_;

private:
  friend access;

  void serialize(serializer& sink) const
  {
    sink << bool_;
  }

  void deserialize(deserializer& source)
  {
    source >> bool_;
  }

  template <typename Iterator>
  friend trial<void> print(bitmap const& bm, Iterator&& out)
  {
    typename Bitstream::size_type last = 0;
    auto i = bm.bool_.begin();
    auto end = bm.bool_.end();

    while (i != end)
    {
      auto delta = *i - last;
      last = *i + 1;

      for (decltype(delta) zero = 0; zero < delta; ++zero)
      {
        print("0\n", out);
      }

      *out++ = '1';
      if (++i != end)
        *out++ = '\n';
    }

    auto remaining_zeros = last < bm.bool_.size() ? bm.bool_.size() - last : 0;
    for (decltype(last) zero = 0; zero < remaining_zeros; ++zero)
    {
      *out++ = '0';
      if (zero != remaining_zeros)
        *out++ = '\n';
    }

    *out++ = '\n';

    return nothing;
  }

  friend bool operator==(bitmap const& x, bitmap const& y)
  {
    return x.bool_ == y.bool_;
  }
};

} // namespace vast

#endif

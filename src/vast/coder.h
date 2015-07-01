#ifndef VAST_CODER_H
#define VAST_CODER_H

#include <algorithm>
#include <array>
#include <limits>
#include <vector>
#include <type_traits>

#include "vast/operator.h"
#include "vast/detail/decompose.h"
#include "vast/detail/range_eval_opt.h"
#include "vast/util/assert.h"
#include "vast/util/operators.h"

namespace vast {

struct access;

/// The concept class for bitmap coders. A coder offers encoding and decoding of
/// values. It encodes values into its type-specific storage and returns a
/// bitstream as a result of a point-query under a given relational operator.
struct coder
{
  /// Retrieves the number entries in the coder, i.e., the number of rows.
  /// @returns The size of the coder measured in number of entries.
  uint64_t rows() const;

  /// Retrieves the coder-specific bitstream storage.
  auto storage() const;

  /// Encodes a single values multiple times.
  /// @tparam An unsigned integral type.
  /// @param x The value to encode.
  /// @param n The number of time to add *x*.
  /// @returns `true` on success.
  template <typename T>
  bool encode(T x, size_t n = 1);

  /// Decodes a value under a relational operator.
  /// @param x The value to decode.
  /// @param op The relation operator under which to decode *x*.
  /// @returns The bitstream for *x* according to *op*.
  template <typename T>
  auto decode(relational_operator op, T x) const;

  /// Appends another coder to this instance.
  /// @param other The coder to append.
  /// @returns `true` on success.
  bool append(coder const& other);

  /// Aritifically increases the size (i.e., the number of rows) without adding
  /// new elements.
  /// @param n The number of rows to increase the coder by.
  /// @returns `true` on success and `false` if there is not enough space.
  bool stretch(size_t n);
};

/// A coder that wraps a single ::bitstream (and can thus only stores 2 values).
template <typename Bitstream>
class singleton_coder : util::equality_comparable<singleton_coder<Bitstream>>
{
  friend access;

public:
  using bitstream_type = Bitstream;

  friend bool operator==(singleton_coder const& x, singleton_coder const& y)
  {
    return x.bitstream_ == y.bitstream_;
  }

  uint64_t rows() const
  {
    return bitstream_.size();
  }

  Bitstream const& storage() const
  {
    return bitstream_;
  }

  template <typename T>
  bool encode(T x, size_t n = 1)
  {
    return bitstream_.append(n, !! x);
  }

  template <typename T>
  Bitstream decode(relational_operator op, T x) const
  {
    auto result = bitstream_;
    if ((!! x && op == equal) || (! x && op == not_equal))
      return result;
    result.flip();
    return result;
  }

  bool append(singleton_coder const& other)
  {
    return bitstream_.append(other.bitstream_);
  }

  bool stretch(size_t n)
  {
    return bitstream_.append(n, false);
  }

private:
  Bitstream bitstream_;
};

/// Base class for coders using an array of bitstreams.
template <typename Derived, typename Bitstream>
class vector_coder : util::equality_comparable<vector_coder<Derived, Bitstream>>
{
  friend access;

public:
  using bitstream_type = Bitstream;

  vector_coder(size_t cardinality = 0)
    : bitstreams_(cardinality)
  {
  }

  friend bool operator==(vector_coder const& x, vector_coder const& y)
  {
    return x.rows_ == y.rows_ && x.bitstreams_ == y.bitstreams_;
  }

  uint64_t rows() const
  {
    return rows_;
  }

  std::vector<bitstream_type> const& storage() const
  {
    return bitstreams_;
  }

  template <typename T>
  bool encode(T x, size_t n = 1)
  {
    if (std::numeric_limits<uint64_t>::max() - rows_ < n)
      return false;
    derived()->encode_impl(static_cast<std::make_unsigned_t<T>>(x), n);
    rows_ += n;
    return true;
  }

  template <typename T>
  bitstream_type decode(relational_operator op, T x) const
  {
    return derived()->decode_impl(op, static_cast<std::make_unsigned_t<T>>(x));
  }

  bool append(vector_coder const& other)
  {
    if (std::numeric_limits<uint64_t>::max() - rows_ < other.rows())
      return false;
    derived()->append_impl(other);
    rows_ += other.rows_;
    return true;
  }

  bool stretch(size_t n)
  {
    if (std::numeric_limits<uint64_t>::max() - rows_ < n)
      return false;
    rows_ += n;
    return true;
  }

  //
  // Container API
  //

  bitstream_type const& operator[](size_t i) const
  {
    VAST_ASSERT(i < bitstreams_.size());
    return bitstreams_[i];
  }

  size_t size() const
  {
    return bitstreams_.size();
  }

  void resize(size_t size)
  {
    bitstreams_.resize(size);
  }

protected:
  void append_impl_helper(vector_coder const& other, bool bit)
  {
    VAST_ASSERT(bitstreams_.size() == other.bitstreams_.size());
    for (auto i = 0u; i < bitstreams_.size(); ++i)
    {
      bitstreams_[i].append(this->rows() - bitstreams_[i].size(), bit);
      bitstreams_[i].append(other.bitstreams_[i]);
    }
  }

  std::vector<bitstream_type> bitstreams_;

private:
  Derived* derived()
  {
    return static_cast<Derived*>(this);
  }

  Derived const* derived() const
  {
    return static_cast<Derived const*>(this);
  }

  void append_impl(vector_coder const& other)
  {
    append_impl_helper(other, false);
  }

  uint64_t rows_ = 0;
};

/// Encodes each value in its own bitstream.
template <typename Bitstream>
struct equality_coder : vector_coder<equality_coder<Bitstream>, Bitstream>
{
  using super = vector_coder<equality_coder<Bitstream>, Bitstream>;
  using super::super;
  using super::bitstreams_;

  template <typename T>
  void encode_impl(T x, size_t n)
  {
    VAST_ASSERT(x < bitstreams_.size());
    auto& bs = bitstreams_[x];
    bs.append(this->rows() - bs.size(), false);
    bs.append(n, true);
  }

  template <typename T>
  Bitstream decode_impl(relational_operator op, T x) const
  {
    VAST_ASSERT(op == less || op == less_equal || op == equal
                || op == not_equal || op == greater_equal || op == greater);
    VAST_ASSERT(x < bitstreams_.size());
    switch (op)
    {
      default:
        return {this->rows(), false};
      case less:
        {
          if (x == 0)
            return {this->rows(), false};
          auto result = bitstreams_[0];
          for (auto i = 1u; i < x; ++i)
            result |= bitstreams_[i];
          result.append(this->rows() - result.size(), false);
          return result;
        }
      case less_equal:
        {
          auto result = bitstreams_[0];
          for (auto i = 1u; i <= x; ++i)
            result |= bitstreams_[i];
          result.append(this->rows() - result.size(), false);
          return result;
        }
      case equal:
      case not_equal:
        {
          auto result = bitstreams_[x];
          result.append(this->rows() - result.size(), false);
          if (op == not_equal)
            result.flip();
          return result;
        }
      case greater_equal:
        {
          auto result = bitstreams_[x];
          for (auto i = x + 1u; i < bitstreams_.size(); ++i)
            result |= bitstreams_[i];
          result.append(this->rows() - result.size(), false);
          return result;
        }
      case greater:
        {
          if (x >= bitstreams_.size() - 1)
            return {this->rows(), false};
          auto result = bitstreams_[x + 1];
          for (auto i = x + 2u; i < bitstreams_.size(); ++i)
            result |= bitstreams_[i];
          result.append(this->rows() - result.size(), false);
          return result;
        }
    }
  }
};

/// Encodes a value according to an inequalty. Given a value *x* and an index
/// *i* in *[0,N)*, all bits are 0 for i < x and 1 for i >= x.
template <typename Bitstream>
struct range_coder : vector_coder<range_coder<Bitstream>, Bitstream>
{
  using super = vector_coder<range_coder<Bitstream>, Bitstream>;
  using super::super;
  using super::bitstreams_;

  template <typename T>
  void encode_impl(T x, size_t n)
  {
    VAST_ASSERT(x < bitstreams_.size() + 1);
    // TODO: This requires adapating RangeEval-Opt to perform "lazy extension"
    // on complement, which is why it is still commented.
    //
    // Lazy append: we only add 0s until we hit index i of value x. The
    // remaining bitstreams are always 1, by definition of the range coding
    // property i >= x for all i in [0,N).
    //for (auto i = 0; i < x; ++i)
    //{
    //  auto& bs = bitstreams_[i];
    //  if (! bs.append(this->rows() - bs.size(), true))
    //    return true;
    //  if (! bs.append(n, false))
    //    return false;
    //}
    for (auto i = 0u; i < bitstreams_.size(); ++i)
    {
      auto& bs = bitstreams_[i];
      bs.append(this->rows() - bs.size(), true);
      bs.append(n, static_cast<T>(i) >= x);
    }
  }

  template <typename T>
  Bitstream decode_impl(relational_operator op, T x) const
  {
    VAST_ASSERT(op == less || op == less_equal || op == equal
                || op == not_equal || op == greater_equal || op == greater);
    VAST_ASSERT(x < bitstreams_.size() + 1);
    switch (op)
    {
      default:
        return {this->rows(), false};
      case less:
        {
          auto result = bitstreams_[x > 0 ? x - 1 : 0];
          result.append(this->rows() - result.size(), true);
          return result;
        }
      case less_equal:
        {
          auto result = bitstreams_[x];
          result.append(this->rows() - result.size(), true);
          return result;
        }
      case equal:
      case not_equal:
        {
          auto result = bitstreams_[x];
          if (x > 0)
          {
            auto prior = ~bitstreams_[x - 1];
            VAST_ASSERT(prior.size() >= result.size());
            result.append(prior.size() - result.size(), true);
            result &= prior;
          }
          result.append(this->rows() - result.size(), false);
          if (op == not_equal)
            result.flip();
          return result;
        }
      case greater:
        {
          auto result = ~bitstreams_[x];
          result.append(this->rows() - result.size(), false);
          return result;
        }
      case greater_equal:
        {
          auto result = ~bitstreams_[x > 0 ? x - 1 : 0];
          result.append(this->rows() - result.size(), false);
          return result;
        }
    }
  }

  void append_impl(super const& other)
  {
    this->append_impl_helper(other, true);
  }
};

/// Maintains one bitstream per *bit* of the value to encode.
/// For example, adding the value 4 appends a 1 to the bitstream for 2^2 and a
/// 0 to to all other bitstreams.
template <typename Bitstream>
struct bitslice_coder
  : public vector_coder<bitslice_coder<Bitstream>, Bitstream>
{
  using super = vector_coder<bitslice_coder<Bitstream>, Bitstream>;
  using super::super;
  using super::bitstreams_;

  template <typename T>
  void encode_impl(T x, size_t n)
  {
    for (auto i = 0u; i < bitstreams_.size(); ++i)
    {
      auto& bs = bitstreams_[i];
      bs.append(this->rows() - bs.size(), false);
      bs.append(n, ((x >> i) & 1) == 0);
    }
  }

  template <typename T>
  Bitstream decode_impl(relational_operator op, T x) const
  {
    switch (op)
    {
      default:
        break;
      case less:
      case less_equal:
      case greater:
      case greater_equal:
        {
          // RangeEval-Opt for the special case with uniform base 2.
          if (x == std::numeric_limits<T>::min())
          {
            if (op == less)
              return {this->rows(), false};
            else if (op == greater_equal)
              return {this->rows(), true};
          }
          else if (op == less || op == greater_equal)
          {
            --x;
          }
          auto result = x & 1 ? Bitstream{this->rows(), true} : bitstreams_[0];
          for (auto i = 1u; i < bitstreams_.size(); ++i)
            if ((x >> i) & 1)
              result |= bitstreams_[i];
            else
              result &= bitstreams_[i];
          if (op == greater || op == greater_equal || op == not_equal)
            result.flip();
          return result;
        }
      case equal:
      case not_equal:
        {
          auto result = Bitstream{this->rows(), true};
          for (auto i = 0u; i < bitstreams_.size(); ++i)
          {
            auto& bs = bitstreams_[i];
            result &= ((x >> i) & 1) ? ~bs : bs;
          }
          if (op == not_equal)
            result.flip();
          return result;
        }
      case in:
      case not_in:
        {
          if (x == 0)
            break;
          x = ~x;
          auto result = Bitstream{this->rows(), false};
          for (auto i = 0u; i < bitstreams_.size(); ++i)
            if (((x >> i) & 1) == 0)
              result |= bitstreams_[i];
          if (op == in)
            result.flip();
          return result;
        }
    }
    return {this->rows(), false};
  }
};

template <typename T>
struct is_singleton_coder : std::false_type { };

template <typename BS>
struct is_singleton_coder<singleton_coder<BS>> : std::true_type { };

template <typename T>
struct is_equality_coder : std::false_type { };

template <typename BS>
struct is_equality_coder<equality_coder<BS>> : std::true_type { };

template <typename T>
struct is_range_coder : std::false_type { };

template <typename BS>
struct is_range_coder<range_coder<BS>> : std::true_type { };

template <typename T>
struct is_bitslice_coder : std::false_type { };

template <typename BS>
struct is_bitslice_coder<bitslice_coder<BS>> : std::true_type { };

/// A multi-component (or multi-level) coder expresses values as a linear
/// combination according to a base vector. This helps significantly to control
/// the index size with high-cardinality attributes.
template <typename Base, typename Coder>
class multi_level_coder :
    util::equality_comparable<multi_level_coder<Base, Coder>>
{
  friend access;

public:
  using base_type = Base;
  using coder_type = Coder;
  using bitstream_type = typename coder_type::bitstream_type;

  template <typename C>
  using coder_array = std::array<C, base_type::components>;

  friend bool operator==(multi_level_coder const& x, multi_level_coder const& y)
  {
    return x.coders_ == y.coders_;
  }

  multi_level_coder()
  {
    initialize(coders_);
  }

  uint64_t rows() const
  {
    return coders_[0].rows();
  }

  coder_array<Coder> const& storage() const
  {
    return coders_;
  }

  template <typename T>
  bool encode(T x, size_t n = 1)
  {
    return encode(coders_, static_cast<std::make_unsigned_t<T>>(x), n);
  }

  template <typename T>
  auto decode(relational_operator op, T x) const
  {
    return decode(coders_, op, static_cast<std::make_unsigned_t<T>>(x));
  }

  bool append(multi_level_coder const& other)
  {
    for (auto i = 0u; i < coders_.size(); ++i)
      if (! coders_[i].append(other.coders_[i]))
        return false;
    return true;
  }

  bool stretch(size_t n)
  {
    for (auto& c : coders_)
      if (! c.stretch(n))
        return false;
    return true;
  }

private:
  // TODO:
  // We could further optimze the number of bitstreams per coder: any base b
  // requires only b-1 bitstreams because one can obtain any bitstream through
  // conjunction/disjunction of the others. While this decreases space
  // requirements by a factor of 1/b, it increases query time by b-1.

  template <typename Bitstream>
  void initialize(coder_array<singleton_coder<Bitstream>>&)
  {
    // Nothing to for singleton coders.
  }

  template <typename Bitstream>
  void initialize(coder_array<range_coder<Bitstream>>& coders)
  {
    // For range coders it suffices to use b-1 bitstreams because the last
    // bitstream always consists of all 1s and is hence superfluous.
    for (auto i = 0u; i < base_type::components; ++i)
      coders[i] = range_coder<Bitstream>{base_type::values[i] - 1};
  }

  template <typename C>
  void initialize(coder_array<C>& coders)
  {
    // All other multi-bitstream coders use one bitstream per unique value.
    for (auto i = 0u; i < base_type::components; ++i)
      coders[i] = C{base_type::values[i]};
  }

  template <typename C, typename T>
  bool encode(coder_array<C>& coders, T x, size_t n = 1)
  {
    auto xs = detail::decompose(x, base_type::values);
    for (auto i = 0u; i < base_type::components; ++i)
      if (! coders[i].encode(xs[i], n))
        return false;
    return true;
  }

  template <typename C, typename T>
  auto decode(coder_array<C> const& coders, relational_operator op, T x) const
    -> std::enable_if_t<is_range_coder<C>{}, typename C::bitstream_type>
  {
    VAST_ASSERT(! (op == in || op == not_in));
    return detail::range_eval_opt<base_type>(coders, op, x);
  }

  template <typename C, typename T>
  auto decode(coder_array<C> const& coders, relational_operator op, T x) const
    -> std::enable_if_t<
         is_equality_coder<C>{} || is_bitslice_coder<C>{},
         typename C::bitstream_type
       >
  {
    VAST_ASSERT(op == equal || op == not_equal || op == in || op == not_in);
    auto xs = detail::decompose(x, base_type::values);
    auto result = coders[0].decode(equal, xs[0]);
    for (auto i = 1u; i < base_type::components; ++i)
      result &= coders[i].decode(equal, xs[i]);
    if (op == not_equal || op == not_in)
      result.flip();
    return result;
  }

  coder_array<coder_type> coders_;
};

template <typename T>
struct is_multi_level_coder : std::false_type { };

template <typename B, typename C>
struct is_multi_level_coder<multi_level_coder<B, C>> : std::true_type { };

} // namespace vast

#endif

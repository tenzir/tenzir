#ifndef VAST_BITMAP_H
#define VAST_BITMAP_H

#include <unordered_map>
#include "vast/bitstream.h"
#include "vast/option.h"

namespace vast {
namespace detail {

struct storage_policy
{
  size_t rows = 0;
};

/// A linked-list-plus-hash-table-based bit stream storage policy.
/// This storage policy offers *O(1)* lookup and *O(log(n))* bounds checks, at
/// the cost of *O(n * b + n)* space.
template <typename T, typename Bitstream>
struct list_storage : storage_policy
{
  typedef Bitstream bitstream_type;

  Bitstream const* find(T const& x) const
  {
    auto i = map_.find(x);
    return i == map_.end() ? nullptr : &i->second->second;
  }

  std::pair<Bitstream const*, Bitstream const*>
  bounds(T const& x) const
  {
    if (map_.empty())
      return {nullptr, nullptr};

    auto i = std::equal_range(
        list_.begin(), list_.end(), list_value_type(x, {}), key_comp_);

    // Note that std::lower_bound returns the first element that is *not* less
    // than x, but we want the element that *is* less than x, and nullptr
    // otherwise.
    auto upper = i.second != list_.end() ? &i.second->second : nullptr;
    auto lower = i.first != list_.begin() && (--i.first)->first < x
      ? &i.first->second 
      : nullptr;

    return {lower, upper};
  }

  void each(std::function<void(T const&, Bitstream&)> f)
  {
    for (auto& p : list_)
      f(p.first, p.second);
  }

  void each(std::function<void(T const&, Bitstream const&)> f) const
  {
    for (auto& p : list_)
      f(p.first, p.second);
  }

  bool emplace(T const& x, Bitstream b)
  {
    auto i = std::lower_bound(
        list_.begin(), list_.end(), list_value_type(x, {}), key_comp_);
    if (i == list_.end() || x < i->first)
    {
      i = list_.emplace(i, x, std::move(b));
      return map_.emplace(x, i).second;
    }
    return false;
  }

  size_t cardinality() const
  {
    return list_.size();
  }

private:
  typedef std::list<std::pair<T, Bitstream>> list_type;
  typedef typename list_type::value_type list_value_type;
  typedef typename list_type::iterator iterator_type;

  struct key_comp
  {
    bool operator()(list_value_type const& x, list_value_type const& y)
    {
      return x.first < y.first;
    };
  };

  list_type list_;
  key_comp key_comp_;
  std::unordered_map<T, iterator_type> map_;
};

/// A purely hash-table-based bit stream storage policy.
/// This storage policy offers *O(1)* lookup and *O(n)* bounds check,
/// requiring *O(n * b)* space.
template <typename T, typename Bitstream>
struct unordered_storage : storage_policy
{
  typedef Bitstream bitstream_type;

  Bitstream const* find(T const& x) const
  {
    auto i = map_.find(x);
    return i == map_.end() ? nullptr : &i->second;
  }

  std::pair<Bitstream const*, Bitstream const*>
  bounds(T const& x) const
  {
    if (map_.empty())
      return {nullptr, nullptr};

    auto l = map_.end();
    auto u = map_.end();
    for (auto p = map_.begin(); p != map_.end(); ++p)
    {
      if (p->first > x && (u == map_.end() || p->first < u->first))
        u = p;
      if (p->first < x && (l == map_.end() || p->first > u->first))
        l = p;
    }

    return {l == map_.end() ? nullptr : &l->second,
            u == map_.end() ? nullptr : &u->second};
  }

  void each(std::function<void(T const&, Bitstream&)> f)
  {
    for (auto& p : map_)
      f(p.first, p.second);
  }

  void each(std::function<void(T const&, Bitstream const&)> f) const
  {
    for (auto& p : map_)
      f(p.first, p.second);
  }

  bool emplace(T const& x, Bitstream b)
  {
    auto p = map_.emplace(x, std::move(b));
    return p.second;
  }

  size_t cardinality() const
  {
    return map_.size();
  }

private:
  std::unordered_map<T, Bitstream> map_;
};

} // detail

/// An equality encoding policy for bitmaps.
template <typename T>
struct equality_encoder
{
  template <typename Storage>
  bool encode(Storage& store, T const& x)
  {
    if (! store.find(x))
      if (! store.emplace(x, {store.rows, 0}))
        return false;

    typedef typename Storage::bitstream_type bs_type;
    store.each([&](T const& k, bs_type& bs) { bs.push_back(x == k); });
    ++store.rows;
    return true;
  }

  template <typename Storage>
  option<typename Storage::bitstream_type>
  decode(Storage& store, T const& x) const
  {
    auto result = store.find(x);
    if (result)
      return *result;
    return {};
  }
};

/// A binary encoding policy for bitmaps. This scheme is also known as
/// *bit-sliced* encoding.
template <typename T>
struct binary_encoder
{
  static size_t constexpr bits = std::numeric_limits<T>::digits;

  template <typename Storage>
  bool encode(Storage& store, T const& x)
  {
    if (! initialized)
    {
      initialized = true;
      assert(store.cardinality() == 0);
      for (size_t i = 0; i < bits; ++i)
        store.emplace(i, {});
    }

    typedef typename Storage::bitstream_type bs_type;
    store.each([&](T const& k, bs_type& bs) { bs.push_back((x >> k) & 1); });

    ++store.rows;
    return true;
  }

  template <typename Storage>
  option<typename Storage::bitstream_type>
  decode(Storage& store, T const& x) const
  {
    typename Storage::bitstream_type result;
    auto found = false;
    for (size_t i = 0; i < bits; ++i)
      if ((x >> i) & 1)
      {
        result |= *store.find(i);
        if (! found)
          found = true;
      }

    if (found)
      return std::move(result);
    return {};
  }

  bool initialized = false;
};

/// A less-than-or-equal range encoding policy for bitmaps.
template <typename T>
struct range_encoder
{
  template <typename Storage>
  bool encode(Storage& store, T const& x)
  {
    if (! store.find(x))
      if (! store.emplace(x, make_bitstream(store, x)))
        return false;

    typedef typename Storage::bitstream_type bs_type;
    store.each([&](T const& k, bs_type& bs) { bs.push_back(x <= k); });
    ++store.rows;
    return true;
  }

  template <typename Storage>
  option<typename Storage::bitstream_type>
  decode(Storage& store, T const& x) const
  {
    auto result = store.find(x);
    if (result)
      return *result;
    return {};
  }

  template <typename Storage>
  typename Storage::bitstream_type
  make_bitstream(Storage& store, T const& x)
  {
    auto range = store.bounds(x);
    if (range.first && range.second)
      return *range.first;
    else if (range.first)
      return {store.rows, true};
    else if (range.second)
      return {store.rows, false};
    else
      return {store.rows, true};
  }
};

/// A null binning policy acting as identity function.
template <typename T>
struct null_binner
{
  T operator()(T x) const
  {
    return std::move(x);
  }
};

/// A binning policy that reduces value to a given precision.
template <typename T>
struct precision_binner
{
  template <typename B>
  using is_bool = typename std::is_same<B, bool>::type;

  template <typename F>
  using is_double = typename std::is_same<F, double>::type;

  static_assert(std::is_arithmetic<T>::value && !is_bool<T>::value,
      "precision binning works only with number types");

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
  precision_binner(int precision)
  {
    integral = std::pow(10, precision < 0 ? -precision : precision);
    if (precision < 0)
      fractional = integral;
  }

  T dispatch(T x, std::true_type) const
  {
    if (fractional != 0.0)
    {
      double i;
      auto f = std::modf(x, &i);
      return i + std::round(f * fractional) / fractional;
    }
    else if (integral)
    {
      return std::round(x / integral);
    }
    
    return x;
  }

  T dispatch(T x, std::false_type) const
  {
    return x / integral;
  }

  T operator()(T x) const
  {
    return dispatch(x, is_double<T>());
  }

  T integral;
  double fractional = 0.0;
};

/// A bitmap which maps values to @link bitstream bitstreams@endlink.
template <
    typename T
  , typename Bitstream = null_bitstream
  , template <typename> class Encoder = equality_encoder
  , template <typename> class Binner = null_binner
>
class bitmap
{
  typedef typename std::conditional<
      std::is_same<Encoder<T>, range_encoder<T>>::value
    , detail::list_storage<T, Bitstream>
    , detail::unordered_storage<T, Bitstream>
  >::type storage_type;

public:
  typedef Bitstream bitstream_type;

  /// Constructs an empty bitmap.
  bitmap(Encoder<T> encoder = Encoder<T>(), Binner<T> binner = Binner<T>())
    : encoder_(encoder)
    , binner_(binner)
  {
  }

  /// Adds a value to the bitmap. For example, in the case of equality
  /// encoding, this entails appending 1 to the single bitstream for the given
  /// value and 0 to all other bitstreams.
  ///
  /// @param x The value to add.
  void push_back(T const& x)
  {
    encoder_.encode(bitstreams_, binner_(x));
  }

  /// Same as lookup().
  option<Bitstream> operator[](T const& x) const
  {
    return lookup(x);
  }

  /// Retrieves a bitstream of a given value.
  ///
  /// @param x The value to find the bitstream for.
  ///
  /// @return An @link option vast::option@endlink containing a bitstream *iff*
  /// the value was found.
  option<Bitstream> lookup(T const& x) const
  {
    return encoder_.decode(bitstreams_, binner_(x));
  }

  /// Retrieves the bitmap size.
  /// @return The number of elements contained in the bitmap.
  size_t size() const
  {
    return bitstreams_.rows;
  }

  /// Checks whether the bitmap is empty.
  /// @return `true` *iff* the bitmap has 0 entries.
  bool empty() const
  {
    return size() == 0;
  }

  /// Transposes the bitmap into a vector of bitmaps where the *i*th element
  /// represents the *i*th row of the encoded bitmap.
  ///
  /// @param header If not `nullptr`, a result parameter receiving the unique
  /// element values in an order such that the *j*th element represents the
  /// *j*th bit in each row.
  ///
  /// @return The transposed bitmap.
  ///
  /// @note The traversal of the bitstream storage must be deterministic in
  /// order for the result to be meaningful.
  std::vector<Bitstream> transpose(std::vector<T>* header) const
  {
    if (header)
      bitstreams_.each(
          [&](T const& x, Bitstream const&) { header->push_back(x); });

    std::vector<Bitstream> rows(bitstreams_.rows);
    for (size_t i = 0; i < rows.size(); ++i)
      bitstreams_.each(
          [&](T const&, Bitstream const& bs) { rows[i].push_back(bs[i]); });

    return rows;
  }

  /// Creates an all-0 or all-1 bitstream compatible in size to the bitmap.
  /// @param bit The value of the bitstream.
  /// @return A bitstream of length size() filled with value *bit*.
  Bitstream all(bool bit) const
  {
    Bitstream bs;
    bs.append(size(), bit);
    return bs;
  }

private:
  Encoder<T> encoder_;
  Binner<T> binner_;
  storage_type bitstreams_;
};

} // namespace vast

#endif

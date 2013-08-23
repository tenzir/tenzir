#ifndef VAST_BITMAP_H
#define VAST_BITMAP_H

#include <list>
#include <unordered_map>
#include "vast/bitstream.h"
#include "vast/exception.h"
#include "vast/operator.h"
#include "vast/option.h"

namespace vast {
namespace detail {

struct storage_policy
{
  size_t rows = 0;
};

/// A vector-based random access bitstream storage policy.
/// This storage policy maps values to indices. It provides *O(1)* access and
/// requires *O(max(T))* space. Hence it is only useful for very dense domains.
template <typename T, typename Bitstream>
struct vector_storage : storage_policy
{
  using bitstream_type = Bitstream;

  Bitstream const* find(T const& x) const
  {
    auto i = static_cast<size_t>(x);
    if (i >= vector_.size() || ! vector_[i])
      return nullptr;
    return &*vector_[i];
  }

  std::pair<Bitstream const*, Bitstream const*> find_bounds(T const& x) const
  {
    if (vector_.empty())
      return {nullptr, nullptr};
    else if (x >= vector_.size())
      return {&*vector_.back(), nullptr};

    Bitstream const* lower = nullptr;
    Bitstream const* upper = nullptr;
    bool found = false;
    for (size_t i = 0; ! found && i < vector_.size(); ++i)
      if (vector_[i] && i < x)
        lower = &*vector_[i];
      else if (lower && i >= x)
        found = true;

    found = false;
    for (size_t i = vector_.size(); ! found && i > 0; --i)
      if (vector_[i - 1] && i - 1 > x)
        upper = &*vector_[i - 1];
      else if (upper && i - 1 < x)
        found = true;

    return {lower, upper};
  }

  void each(std::function<void(T const&, Bitstream&)> f)
  {
    for (size_t i = 0; i < vector_.size(); ++i)
      f(i, *vector_[i]);
  }

  void each(std::function<void(T const&, Bitstream const&)> f) const
  {
    for (size_t i = 0; i < vector_.size(); ++i)
      f(i, *vector_[i]);
  }

  bool insert(T const& x, Bitstream b = {})
  {
    auto i = static_cast<size_t>(x);
    if (i >= vector_.size())
      vector_.resize(i + 1);
    else if (i < vector_.size() && vector_[i])
      return false;

    vector_[i] = std::move(b);
    ++cardinality_;
    return true;
  }

  size_t cardinality() const
  {
    return cardinality_;
  }

private:
  std::vector<option<Bitstream>> vector_;
  size_t cardinality_ = 0;
};

/// A linked-list-plus-hash-table-based bitstream storage policy.
/// This storage policy offers *O(1)* lookup and *O(log(n))* bounds checks, at
/// the cost of *O(n * b + n)* space.
template <typename T, typename Bitstream>
struct list_storage : storage_policy
{
  using bitstream_type = Bitstream;

  Bitstream const* find(T const& x) const
  {
    auto i = map_.find(x);
    return i == map_.end() ? nullptr : &i->second->second;
  }

  std::pair<Bitstream const*, Bitstream const*> find_bounds(T const& x) const
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

  bool insert(T const& x, Bitstream b = {})
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
  using list_type = std::list<std::pair<T, Bitstream>>;
  using list_value_type = typename list_type::value_type;
  using iterator_type = typename list_type::iterator;

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

/// A purely hash-table-based bitstream storage policy.
/// This storage policy offers *O(1)* lookup and *O(n)* bounds check,
/// requiring *O(n * b)* space.
template <typename T, typename Bitstream>
struct unordered_storage : storage_policy
{
  using bitstream_type = Bitstream;

  Bitstream const* find(T const& x) const
  {
    auto i = map_.find(x);
    return i == map_.end() ? nullptr : &i->second;
  }

  std::pair<Bitstream const*, Bitstream const*> find_bounds(T const& x) const
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

  bool insert(T const& x, Bitstream b = {})
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

template <typename T, typename Bitstream, typename Storage>
struct coder
{
  bool patch(size_t n = 1)
  {
    auto success = true;
    store.each(
        [&](T const&, Bitstream& bs)
        {
          if (! bs.append(n, false))
            success = false;
        });
    return success;
  }

  Storage store;
};

/// An equality encoding policy for bitmaps.
template <typename T, typename Bitstream>
struct equality_coder
  : coder<T, Bitstream, detail::unordered_storage<T, Bitstream>>
{
  using super = coder<T, Bitstream, detail::unordered_storage<T, Bitstream>>;
  using super::store;

  bool encode(T const& x)
  {
    if (! store.find(x))
      if (! store.insert(x, {store.rows, 0}))
        return false;

    store.each([&](T const& k, Bitstream& bs) { bs.push_back(x == k); });
    ++store.rows;
    return true;
  }

  option<Bitstream> decode(T const& x, relational_operator op = equal) const
  {
    auto result = store.find(x);
    switch (op)
    {
      default:
        throw error::operation("unsupported relational operator", op);
      case equal:
        if (result)
          return *result;
        else
          return {};
      case not_equal:
        if (result)
          return ~*result;
        else
          return {{store.rows, true}};
    }
  }
};

/// A binary encoding policy for bitmaps.
/// This scheme is also known as *bit-sliced* encoding.
template <typename T, typename Bitstream>
struct binary_coder
  : coder<T, Bitstream, detail::vector_storage<uint8_t, Bitstream>>
{
  static_assert(std::is_integral<T>::value,
                "binary encoding requires an integral type");

  using super = coder<T, Bitstream, detail::vector_storage<uint8_t, Bitstream>>;
  using super::store;

  static uint8_t constexpr bits = std::numeric_limits<T>::digits;

  binary_coder()
  {
    for (uint8_t i = 0; i < bits; ++i)
      store.insert(i);
  }

  bool encode(T const& x)
  {
    store.each([&](uint8_t i, Bitstream& bs) { bs.push_back((x >> i) & 1); });
    ++store.rows;
    return true;
  }

  option<Bitstream> decode(T const& x, relational_operator op = equal) const
  {
    switch (op)
    {
      default:
        throw error::operation("unsupported relational operator", op);
      case equal:
        {
          Bitstream result{store.rows, true};
          for (uint8_t i = 0; i < bits; ++i)
            result &= ((x >> i) & 1) ? *store.find(i) : ~*store.find(i);
          if (result.find_first() == Bitstream::npos)
            return {};
          else
            return std::move(result);
        }
      case not_equal:
        if (auto result = decode(x, equal))
          return std::move((*result).flip());
        else
          return {{store.rows, true}};
    }
  }
};

/// A less-than-or-equal range encoding policy for bitmaps.
template <typename T, typename Bitstream>
struct range_coder : coder<T, Bitstream, detail::list_storage<T, Bitstream>>
{
  static_assert(! std::is_same<T, bool>::value,
                "range encoding for boolean value does not make sense");

  using super = coder<T, Bitstream, detail::list_storage<T, Bitstream>>;
  using super::store;

  bool encode(T const& x)
  {
    if (! store.find(x))
    {
      auto range = store.find_bounds(x);
      auto success = true;
      if (range.first && range.second)
        success = store.insert(x, {*range.first});
      else if (range.first)
        success = store.insert(x, {store.rows, true});
      else if (range.second)
        success = store.insert(x, {store.rows, false});
      else
        success = store.insert(x, {store.rows, true});
      if (! success)
        return false;
    }

    store.each([&](T const& k, Bitstream& bs) { bs.push_back(x <= k); });
    ++store.rows;
    return true;
  }

  option<Bitstream>
  decode(T const& x, relational_operator op = less_equal) const
  {
    switch (op)
    {
      default:
        throw error::operation("unsupported relational operator", op);
      case less:
        if (! std::is_integral<T>::value)
          throw error::operation("operator needs integral type", op);
        else if (x == std::numeric_limits<T>::lowest())
          return decode(x, less_equal);
        else
          return decode(x - 1, less_equal);
      case less_equal:
        if (auto result = store.find(x))
          return *result;
        else if (auto lower = store.find_bounds(x).first)
          return *lower;
        else
          return {};
      case greater:
        if (auto result = decode(x, less_equal))
          return std::move((*result).flip());
        else
          return {{store.rows, true}};
      case greater_equal:
        if (auto result = decode(x, less))
          return std::move((*result).flip());
        else
          return {{store.rows, true}};
      case equal:
        {
          // For a range-encoded bitstream v == x means z <= x & ~pred(z). If
          // pred(z) does not exist, the query reduces to z <= x.
          auto le = decode(x, less_equal);
          if (! le)
            return {};
          auto range = store.find_bounds(x);
          if (! range.first)
            return le;
          *le &= ~*range.first;
          return le;
        }
      case not_equal:
        if (auto result = decode(x, equal))
          return std::move((*result).flip());
        else
          return {{store.rows, true}};
    }
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
  typename T,
  typename Bitstream = null_bitstream,
  template <typename, typename> class Coder = equality_coder,
  template <typename> class Binner = null_binner
>
class bitmap
{
public:
  using coder_type = Coder<T, Bitstream>;
  using binner_type = Binner<T>;

  /// Constructs an empty bitmap.
  bitmap(binner_type binner = binner_type{}, coder_type coder = coder_type{})
    : coder_(coder), binner_(binner)
  {
  }

  /// Adds a value to the bitmap. For example, in the case of equality
  /// encoding, this entails appending 1 to the single bitstream for the given
  /// value and 0 to all other bitstreams.
  ///
  /// @param x The value to add.
  ///
  /// @return `true` on success and `false` if the bitmap is full, i.e., has
  /// `2^std::numeric_limits<size_t>::digits() - 1` elements.
  bool push_back(T const& x)
  {
    auto success = coder_.encode(binner_(x));
    return success && valid_.push_back(true);
  }

  /// Appends a given number of invalid rows/elements to the bitmaps.
  /// @param n The number of elements to append.
  /// @return `true` on success and `false` if the bitmap is full.
  bool patch(size_t n = 1)
  {
    return valid_.append(n, false) && coder_.patch(n);
  }

  /// Shorthand for `lookup(equal, x)`.
  option<Bitstream> operator[](T const& x) const
  {
    return lookup(equal, x);
  }

  /// Retrieves a bitstream of a given value with respect to a given operator.
  ///
  /// @param op The relational operator to use for looking up *x*.
  ///
  /// @param x The value to find the bitstream for.
  ///
  /// @return An @link option vast::option@endlink containing a bitstream
  /// for all values *v* such that *op(v,x)* is `true`.
  option<Bitstream> lookup(relational_operator op, T const& x) const
  {
    auto result = coder_.decode(binner_(x), op);
    if (result)
      *result &= valid_;
    return result;
  }

  /// Retrieves the raw bistream without decoding the result.
  /// @param x The raw value to lookup.
  /// @return A pointer to the bitstream for *x* or `nullptr` if not found.
  template <typename U>
  Bitstream const* lookup_raw(U const& x) const
  {
    return coder_.store.find(x);
  }

  /// Retrieves the bitmap size.
  /// @return The number of elements contained in the bitmap.
  size_t size() const
  {
    return coder_.store.rows;
  }

  /// Checks whether the bitmap is empty.
  /// @return `true` *iff* the bitmap has 0 entries.
  bool empty() const
  {
    return size() == 0;
  }

  /// Converts a bitmap to a `std::string`.
  ///
  /// @param bm The bitmap to convert.
  ///
  /// @param with_header If `true`, include a header with bitmap values as first
  /// row in the output.
  ///
  /// @param delim The delimiting character separating header values.
  ///
  /// @return A `std::string` representation of *bm*.
  friend std::string
  to_string(bitmap const& bm, bool with_header = true, char delim = '\t')
  {
    if (bm.empty())
      return {};
    std::string str;
    if (with_header)
    {
      using std::to_string;
      //using vast::to_string;
      bm.coder_.store.each(
          [&](T const& x, Bitstream const&) { str += to_string(x) + delim; });
      str.pop_back();
      str += '\n';
    }
    std::vector<Bitstream> cols;
    cols.reserve(bm.coder_.store.rows);
    bm.coder_.store.each(
        [&](T const&, Bitstream const& bs) { cols.push_back(bs); });
    for (auto& row : transpose(cols))
      str += to_string(row) + '\n';
    str.pop_back();
    return str;
  }

private:
  coder_type coder_;
  binner_type binner_;
  Bitstream valid_;
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
  using value_type = bool;
  using bitstream_type = Bitstream;
  using storage_type = Bitstream;

  bitmap() = default;

  bool push_back(bool x)
  {
    auto success = bool_.push_back(x);
    return valid_.push_back(true) && success;
  }

  bool patch(size_t n = 1)
  {
    auto success = bool_.append(n, false);
    return valid_.append(n, false) && success;
  }

  option<Bitstream> operator[](bool x) const
  {
    return lookup(x);
  }

  option<Bitstream> lookup(relational_operator op, bool x) const
  {
    switch (op)
    {
      default:
        throw error::operation("unsupported relational operator", op);
      case not_equal:
        return {(x ? ~bool_ : bool_) & valid_};
      case equal:
        return {(x ? bool_ : ~bool_) & valid_};
    }
  }

  size_t size() const
  {
    return bool_.size();
  }

  bool empty() const
  {
    return bool_.empty();
  }

private:
  friend std::string to_string(bitmap const& bm)
  {
    std::string str;
    auto i = bm.bool_.find_first();
    if (i == Bitstream::npos)
      throw exception("bitstream too large to convert to string");
    str.reserve(bm.bool_.size() * 2);
    if (i > 0)
      for (size_t j = 0; j < i; ++j)
        str += "0\n";
    str += "1\n";
    auto j = i;
    while ((j = bm.bool_.find_next(i)) != Bitstream::npos)
    {
      auto delta = j - i;
      for (i = 1; i < delta; ++i)
        str += "0\n";
      str += "1\n";
      i = j;
    }
    assert(j == Bitstream::npos);
    for (j = 1; j < bm.bool_.size() - i; ++i)
      str += "0\n";
    if (str.back() == '\n')
      str.pop_back();
    return str;
  }

  storage_type bool_;
  Bitstream valid_;
};

} // namespace vast

#endif

#ifndef VAST_BITMAP_H
#define VAST_BITMAP_H

#include <unordered_map>
#include "vast/bitstream.h"

namespace vast {
namespace detail {

/// A link-list-plus-hash-table-based bit stream storage policy.
/// This storage policy offers *O(1)* lookup and *O(1)* neighbor access, at the
/// cost of *O(n * b + n)* space.
template <typename T, typename Bitstream>
struct list_storage
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
/// This storage policy offers *O(1)* lookup and *O(n)* neighbor access,
/// requiring *O(n * b)* space.
template <typename T, typename Bitstream>
struct unordered_storage
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

  bool emplace(T const& x, Bitstream b)
  {
    auto p = map_.emplace(x, std::move(b));
    return p.second;
  }

private:
  std::unordered_map<T, Bitstream> map_;
};

} // detail

/// The base class for bitmap encoding policies.
template <typename T>
struct equality_encoder
{
  bool operator()(T const& x, T const& y)
  {
    return x == y;
  }

  template <typename Storage>
  typename Storage::bitstream_type
  make_bitstream(Storage& /* store */, size_t n, T const& x)
  {
    return {n, 0};
  }
};

/// A less-than-or-equal range encoding policy for bitmaps.
template <typename T>
struct range_encoder
{
  bool operator()(T const& x, T const& y)
  {
    return x <= y;
  }

  template <typename Storage>
  typename Storage::bitstream_type
  make_bitstream(Storage& store, size_t n, T const& x)
  {
    auto range = store.bounds(x);
    if (range.first && range.second)
      return *range.first;
    else if (range.first)
      return {n, true};
    else if (range.second)
      return {n, false};
    else
      return {n, true};
  }
};

/// A null binning policy acting as identity function.
template <typename T>
struct null_binner
{
  T operator()(T x)
  {
    return std::move(x);
  }
};

/// A bitmap.
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
  /// Constructs an empty bitmap.
  bitmap(Encoder<T> encoder = Encoder<T>(), Binner<T> binner = Binner<T>())
    : encoder_(encoder)
    , binner_(binner)
  {
  }

  /// Adds a value to the bitmap. This entails appending 1 to the single
  /// bitstream for the given value and 0 to all other bitstreams.
  ///
  /// @param x The value to add.
  void push_back(T const& x)
  {
    auto bin = binner_(x);
    if (! bitstreams_.find(bin))
    {
      auto z = encoder_.make_bitstream(bitstreams_, num_elements_, bin);
      auto success = bitstreams_.emplace(bin, std::move(z));
      assert(success);
    }

    bitstreams_.each(
        [&](T const& k, Bitstream& v) { v.push_back(encoder_(bin, k)); });

    ++num_elements_;
  }

  Bitstream const* operator[](T const& x) const
  {
    return bitstreams_.find(x);
  }

private:
  Encoder<T> encoder_;
  Binner<T> binner_;
  storage_type bitstreams_;
  size_t num_elements_ = 0;
};

} // namespace vast

#endif

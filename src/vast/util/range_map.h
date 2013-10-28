#ifndef VAST_UTIL_INTERVAL_MAP_H
#define VAST_UTIL_INTERVAL_MAP_H

#include <cassert>
#include <map>

namespace vast {
namespace util {

/// An associative data structure that maps half-open intervals to values and
/// supports point lookups.
template <typename Point, typename Value>
class range_map
{
public:
  /// Associates a value with a right-open range.
  /// @param l The left endpoint of the interval.
  /// @param r The right endpoint of the interval.
  /// @param v The value r associate with *[l, r]*.
  /// @returns `true` on success.
  bool insert(Point l, Point r, Value v)
  {
    assert(l < r);
    auto lb = map_.lower_bound(l);
    if (find(l, lb) != map_.end())
      return false;
    if (lb == map_.end() || lb->first >= r)
      return map_.emplace(
          std::move(l), std::make_pair(std::move(r), std::move(v))).second;
    return false;
  }

  /// Removes a value given a point from a right-open range.
  ///
  /// @param p A point from a range that maps to a value.
  ///
  /// @returns `true` if the value associated with the interval containing *p*
  /// has been successfully removed, and `false` if *p* does not map to an
  /// existing value.
  bool erase(Point p)
  {
    auto i = find(p, map_.lower_bound(p));
    if (i == map_.end())
      return false;
    map_.erase(i);
    return true;
  } 

  /// Retrieves a value for a given point.
  ///
  /// @param p The point to lookup.
  ///
  /// @returns A pointer to the value associated with the half-open interval
  /// *[a,b)* iff *a <= p < b*, and `nullptr` otherwise.
  Value const* lookup(Point const& p) const
  {
    auto i = find(p, map_.lower_bound(p));
    return i != map_.end() ? &i->second.second : nullptr;
  }

  /// Applies a function over each range and associated value.
  /// @param f The function to apply.
  void
  each(std::function<void(Point const&, Point const&, Value const&)> f) const
  {
    for (auto& p : map_)
      f(p.first, p.second.first, p.second.second);
  }

  /// Retrieves the size of the range map.
  /// @returns The number of entries in the map.
  size_t size() const
  {
    return map_.size();
  }

  /// Checks whether the range map is empty.
  /// @return `true` iff the map is empty.
  bool empty() const
  {
    return map_.empty();
  }

private:
  using map_type = std::map<Point, std::pair<Point, Value>>;
  using map_const_iterator = typename map_type::const_iterator;

  map_const_iterator find(Point const& p, map_const_iterator lb) const
  {
    if ((lb != map_.end() && lb->first == p) ||
        (lb != map_.begin() && p < (--lb)->second.first))
      return lb;
    return map_.end();
  }

  map_type map_;
};

} // namespace util
} // namespace vast

#endif

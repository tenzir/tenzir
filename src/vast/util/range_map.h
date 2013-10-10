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
  bool insert(Point l, Point r, Value v)
  {
    assert(l < r);
    auto lb = map_.lower_bound(l);
    if (find(l, lb))
      return false;
    if (lb == map_.end() || (++lb != map_.end() && lb->first >= r))
      return map_.emplace(
          std::move(l), std::make_pair(std::move(r), std::move(v))).second;
    return false;
  }

  /// Retrieves a value for a given point.
  ///
  /// @param p The point to lookup.
  ///
  /// @return A pointer to the value associated with the half-open interval
  /// *[a,b)* iff *a <= p < b*, and `nullptr` otherwise.
  Value const* lookup(Point const& p) const
  {
    return find(p, map_.lower_bound(p));
  }

  /// Applies a function over each range and associated value.
  /// @param f The function to apply.
  void
  each(std::function<void(Point const&, Point const&, Value const&)> f) const
  {
    for (auto& p : map_)
      f(p.first, p.second.first, p.second.second);
  }

private:
  using map_type = std::map<Point, std::pair<Point, Value>>;

  Value const* find(Point const& p, typename map_type::const_iterator lb) const
  {
    if ((lb != map_.end() && lb->first == p) ||
        (lb != map_.begin() && p < (--lb)->second.first))
      return &lb->second.second;
    return nullptr;
  }

  std::map<Point, std::pair<Point, Value>> map_;
};

} // namespace util
} // namespace vast

#endif

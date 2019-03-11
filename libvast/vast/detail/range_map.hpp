/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#pragma once

#include <map>
#include <tuple>

#include "vast/detail/assert.hpp"
#include "vast/detail/iterator.hpp"

namespace vast::detail {

/// An associative data structure that maps half-open, *disjoint* intervals to
/// values.
template <class Point, class Value>
class range_map {
  static_assert(std::is_arithmetic_v<Point>,
                "Point must be an arithmetic type");

  using map_type = std::map<Point, std::pair<Point, Value>>;
  using map_iterator = typename map_type::iterator;
  using map_const_iterator = typename map_type::const_iterator;

public:
  struct entry {
    const Point& left;
    const Point& right;
    const Value& value;
  };

  class const_iterator
    : public iterator_adaptor<
        const_iterator,
        map_const_iterator,
        std::tuple<Point, Point, Value>,
        std::bidirectional_iterator_tag,
        entry
      > {
    using super = iterator_adaptor<
      const_iterator,
      map_const_iterator,
      std::tuple<Point, Point, Value>,
      std::bidirectional_iterator_tag,
      entry
    >;

  public:
    using super::super;

  private:
    friend iterator_access;

    entry dereference() const {
      return {this->base()->first, this->base()->second.first,
                      this->base()->second.second};
    }
  };

  const_iterator begin() const {
    return const_iterator{map_.begin()};
  }

  const_iterator end() const {
    return const_iterator{map_.end()};
  }

  /// @returns the underlying container.
  map_type& container() {
    return map_;
  }

  /// Associates a value with a right-open range.
  /// @param l The left endpoint of the interval.
  /// @param r The right endpoint of the interval.
  /// @param v The value r associated with *[l, r]*.
  /// @returns `true` on success.
  bool insert(Point l, Point r, Value v) {
    VAST_ASSERT(l < r);
    auto lb = map_.lower_bound(l);
    if (locate(l, lb) == map_.end() && (lb == map_.end() || r <= left(lb)))
      return map_.emplace(l, std::make_pair(r, std::move(v))).second;
    return false;
  }

  /// Inserts a value for a right-open range, updating existing adjacent
  /// intervals if it's possible to merge them. Two intervals can only be
  /// merged if they have the same values.
  /// @note If *[l,r]* reaches into an existing interval, injection fails.
  /// @param l The left endpoint of the interval.
  /// @param r The right endpoint of the interval.
  /// @param v The value r associated with *[l,r]*.
  /// @returns `true` on success.
  bool inject(Point l, Point r, Value v) {
    VAST_ASSERT(l < r);
    if (map_.empty())
      return emplace(l, r, std::move(v));
    auto i = map_.lower_bound(l);
    // Adjust position (i = this, p = prev, n = next).
    if (i == map_.end() || (i != map_.begin() && l != left(i)))
      --i;
    auto n = i;
    ++n;
    auto p = i;
    if (i != map_.begin())
      --p;
    else
      p = map_.end();
    // Assess the fit.
    auto fits_left = r <= left(i) && (p == map_.end() || l >= right(p));
    auto fits_right = l >= right(i) && (n == map_.end() || r <= left(n));
    if (fits_left) {
      auto right_merge = r == left(i) && v == value(i);
      auto left_merge = p != map_.end() && l == right(p) && v == value(p);
      if (left_merge && right_merge) {
        right(p) = right(i);
        map_.erase(i);
      } else if (left_merge) {
        right(p) = r;
      } else if (right_merge) {
        emplace(l, right(i), std::move(v));
        map_.erase(i);
      } else {
        emplace(l, r, std::move(v));
      }
      return true;
    } else if (fits_right) {
      auto right_merge = n != map_.end() && r == left(n) && v == value(n);
      auto left_merge = l == right(i) && v == value(i);
      if (left_merge && right_merge) {
        right(i) = right(n);
        map_.erase(n);
      } else if (left_merge) {
        right(i) = r;
      } else if (right_merge) {
        emplace(l, right(n), std::move(v));
        map_.erase(n);
      } else {
        emplace(l, r, std::move(v));
      }
      return true;
    }
    return false;
  }

  /// Removes a value given a point from a right-open range.
  /// @param p A point from a range that maps to a value.
  /// @returns `true` if the value associated with the interval containing *p*
  ///          has been successfully removed, and `false` if *p* does not map
  ///          to an existing value.
  bool erase(Point p) {
    auto i = locate(p, map_.lower_bound(p));
    if (i == map_.end())
      return false;
    map_.erase(i);
    return true;
  }

  /// Adjusts or erases ranges so that no values in the map overlap with
  /// *[l,r)*.
  /// @param l The left endpoint of the interval.
  /// @param r The right endpoint of the interval.
  void erase(Point l, Point r) {
    if (l > r)
      return;
    auto next_left = l;
    for (;;) {
      auto lb = map_.lower_bound(next_left);
      auto i = locate(next_left, lb);
      if (i == map_.end())
        i = lb;
      if (i == map_.end() || left(i) >= r)
        break;
      next_left = right(i);
      if (l <= left(i) && r >= right(i)) { // [l,r) overlaps [i) in its entirety
        map_.erase(i);
      } else if (left(i) <= l
                 && right(i) >= r) { // [i) overlaps [l,r) in its entirety
        Point orig_r = right(i);
        right(i) = l;
        inject(r, orig_r, value(i));
        break;
      } else if (l <= left(i) && r > left(i)) { // [l,r) overlaps [i) partially
                                                // and starts before
        map_.emplace(r, std::make_pair(right(i), std::move(value(i))));
        map_.erase(i);
        break;
      } else if (l < right(i) && r >= right(i)) { // [l,r) overlaps [i)
                                                  // partially and starts after
        right(i) = l;
      }
    }
  }

  /// Retrieves the value for a given point.
  /// @param p The point to lookup.
  /// @returns A pointer to the value associated with the half-open interval
  ///          *[a,b)* if *a <= p < b* and `nullptr` otherwise.
  const Value* lookup(const Point& p) const {
    auto i = locate(p, map_.lower_bound(p));
    return i != map_.end() ? &i->second.second : nullptr;
  }

  /// Retrieves value and interval for a given point.
  /// @param p The point to lookup.
  /// @returns A tuple with the last component holding a pointer to the value
  ///          associated with the half-open interval *[a,b)* if *a <= p < b*,
  ///          and `nullptr` otherwise. If the last component points to a
  ///          valid value, then the first two represent *[a,b)* and *[0,0)*
  ///          otherwise.
  std::tuple<Point, Point, const Value*> find(const Point& p) const {
    auto i = locate(p, map_.lower_bound(p));
    if (i == map_.end())
      return {0, 0, nullptr};
    else
      return {left(i), right(i), &i->second.second};
  }

  /// Retrieves the size of the range map.
  /// @returns The number of entries in the map.
  size_t size() const {
    return map_.size();
  }

  /// Checks whether the range map is empty.
  /// @returns `true` iff the map is empty.
  bool empty() const {
    return map_.empty();
  }

  /// Clears the range map.
  void clear() {
    return map_.clear();
  }

  template <class Inspector>
  friend auto inspect(Inspector& f, range_map& m) {
    return f(m.map_);
  }

private:
  template <class Iterator>
  static auto& left(Iterator i) {
    return i->first;
  }

  template <class Iterator>
  static auto& right(Iterator i) {
    return i->second.first;
  }

  template <class Iterator>
  static auto& value(Iterator i) {
    return i->second.second;
  }

  // Finds the interval of a point.
  map_const_iterator locate(const Point& p, map_const_iterator lb) const {
    if ((lb != map_.end() && p == left(lb))
        || (lb != map_.begin() && p < right(--lb)))
      return lb;
    return map_.end();
  }

  map_iterator locate(const Point& p, map_iterator lb) {
    if ((lb != map_.end() && p == left(lb))
        || (lb != map_.begin() && p < right(--lb)))
      return lb;
    return map_.end();
  }

  bool emplace(Point l, Point r, Value v) {
    auto pair = std::make_pair(std::move(r), std::move(v));
    return map_.emplace(std::move(l), std::move(pair)).second;
  }

  map_type map_;
};

} // namespace vast::detail


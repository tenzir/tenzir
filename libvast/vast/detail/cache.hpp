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

#include <cstddef>
#include <functional>
#include <list>
#include <unordered_map>
#include <type_traits>

#include <caf/meta/load_callback.hpp>

#include "vast/error.hpp"

#include "vast/detail/assert.hpp"
#include "vast/detail/operators.hpp"
#include "vast/detail/type_traits.hpp"

namespace vast::detail {

struct lru;

/// A direct-mapped cache with fixed capacity.
template <class Key, class Value, class Policy = lru>
class cache : equality_comparable<cache<Key, Value, Policy>> {
public:
  using key_type = Key;
  using mapped_type = Value;
  using value_type = std::pair<const Key, Value>;
  using iterator = typename std::list<value_type>::iterator;
  using const_iterator = typename std::list<value_type>::const_iterator;

  /// The cache order and evicition policy.
  using policy = Policy;

  /// The callback to invoke for evicted elements.
  using evict_callback = std::function<void(key_type&, mapped_type&)>;

  /// Constructs an LRU cache with a maximum number of elements.
  /// @param capacity The maximum number of elements in the cache.
  /// @pre `capacity > 0`
  cache(size_t capacity = 100) : capacity_{capacity} {
    VAST_ASSERT(capacity_ > 0);
  }

  // -- capacity -------------------------------------------------------------

  /// Sets a callback for elements to be evicted.
  /// @param fun The function to invoke with the element being evicted.
  void on_evict(evict_callback fun) {
    on_evict_ = fun;
  }

  /// Manually evicts an element.
  /// @returns The evicted key-value pair.
  /// @pre `!empty()`
  value_type evict() {
    VAST_ASSERT(!empty());
    auto i = tracker_.find(xs_.front().first);
    VAST_ASSERT(i != tracker_.end());
    tracker_.erase(i);
    auto victim = std::move(xs_.front());
    xs_.pop_front();
    if (on_evict_)
      on_evict_(const_cast<key_type&>(victim.first), victim.second);
    return victim;
  }

  /// Retrieves the maximum number elements the cache can hold.
  /// @returns The cache's capacity.
  size_t capacity() const {
    return capacity_;
  }

  /// Adjusts the cache capacity and evicts elements if the new capacity is
  /// smaller than the previous one.
  /// @param c the new capacity.
  /// @pre `c > 0`
  void capacity(size_t c) {
    VAST_ASSERT(c > 0);
    capacity_ = c;
    auto n = size();
    for (auto i = c; i < n; ++i)
      evict();
  }

  /// Retrieves the current number of elements in the cache.
  /// @returns The number of elements in the cache.
  size_t size() const {
    return xs_.size();
  }

  /// Checks whether the cache is empty.
  /// @returns `true` iff the cache holds no elements.
  bool empty() const {
    return xs_.empty();
  }

  // -- iterators -----------------------------------------------------------

  auto begin() {
    return xs_.begin();
  }

  auto begin() const {
    return xs_.begin();
  }

  auto end() {
    return xs_.end();
  }

  auto end() const {
    return xs_.end();
  }

  auto rbegin() {
    return xs_.rbegin();
  }

  auto rbegin() const {
    return xs_.rbegin();
  }

  auto rend() {
    return xs_.rend();
  }

  auto rend() const {
    return xs_.rend();
  }

  // -- element access-------------------------------------------------------

  /// Accesses the value for a given key. If key does not exists,
  /// the function default-constructs a value of `mapped_type`.
  /// @param key The key to lookup.
  /// @returns The value corresponding to *key*.
  mapped_type& operator[](const key_type& x) {
    auto i = tracker_.find(x);
    if (i == tracker_.end())
      return insert({x, {}}).first->second;
    policy::access(xs_, i->second);
    return i->second;
  }

  // -- modifiers -----------------------------------------------------------

  /// Inserts a fresh entry in the cache.
  /// @param key The key mapping to *value*.
  /// @param value The value for *key*.
  /// @returns An pair of an iterator and boolean flag that indicates whether
  ///          the entry has been added successfully.
  template <class T>
  auto insert(T&& x)
  -> std::enable_if_t<
    std::is_same_v<std::decay_t<T>, value_type>,
    std::pair<iterator, bool>
  > {
    auto i = tracker_.find(x.first);
    if (i != tracker_.end()) {
      policy::access(xs_, i->second);
      return {i->second, false};
    }
    if (size() == capacity_)
      evict();
    auto j = policy::insert(xs_, std::forward<T>(x));
    tracker_.emplace(j->first, j);
    return {j, true};
  }

  template <class... Ts>
  std::pair<iterator, bool> emplace(Ts&&... xs) {
    return insert(value_type{std::forward<Ts>(xs)...});
  }

  /// Removes an entry for a given key without invoking the eviction callback.
  /// @param x The key to remove.
  /// @returns The number of entries removed.
  size_t erase(const key_type& x) {
    auto i = tracker_.find(x);
    if (i == tracker_.end())
      return 0;
    xs_.erase(i->second);
    tracker_.erase(i);
    return 1;
  }

  /// Removes an entry for a given key without invoking the eviction callback.
  void erase(iterator i) {
    auto j = tracker_.begin();
    while (j != tracker_.end()) {
      if (j->second == i)
        j = tracker_.erase(j);
      else
        ++j;
    }
    xs_.erase(i);
  }

  /// Removes all elements from the cache.
  void clear() {
    xs_.clear();
    tracker_.clear();
  }

  // -- lookup --------------------------------------------------------------

  auto find(const key_type& x) {
    auto i = tracker_.find(x);
    if (i == tracker_.end())
      return xs_.end();
    policy::access(xs_, i->second);
    return i->second;
  }

  size_t count(const key_type& x) {
    return find(x) == end() ? 0 : 1;
  }

  // -- concepts ------------------------------------------------------------

  template <class Inspector>
  friend auto inspect(Inspector& f, cache& c) {
    auto load = [&]() -> error {
      for (auto i = c.xs_.begin(); i != c.xs_.end(); ++i)
        c.tracker_.emplace(i->first, i);
      return {};
    };
    return f(c.xs_, c.capacity_, caf::meta::load_callback(load));
  }

  friend bool operator==(const cache& x, const cache& y) {
    return x.xs_ == y.xs_ && x.capacity_ == y.capacity_;
  }

private:
  std::list<value_type> xs_;
  std::unordered_map<key_type, iterator> tracker_;
  evict_callback on_evict_;
  size_t capacity_;
};

/// A *least recently used* (LRU) cache eviction policy.
struct lru {
  template <class List, class Iterator>
  static void access(List& xs, Iterator i) {
    xs.splice(xs.end(), xs, i);
  }

  template <class List, class T>
  static auto insert(List& xs, T&& x) {
    return xs.insert(xs.end(), std::forward<T>(x));
  }
};

/// A *most recently used* (MRU) cache eviction policy.
struct mru {
  template <class List, class Iterator>
  static void access(List& xs, Iterator i) {
    xs.splice(xs.begin(), xs, i);
  }

  template <class List, class T>
  static auto insert(List& xs, T&& x) {
    return xs.insert(xs.begin(), std::forward<T>(x));
  }
};

} // namespace vast::detail


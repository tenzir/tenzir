//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

// This file comes from a 3rd party and has been adapted to fit into the Tenzir
// code base. Details about the original file:
//
// - Repository: https://github.com/lamerman/cpp-lru-cache
// - Commit:     de1c4a03569bf3bd540e7f55ab5c2961411dbe22
// - Path:       include/lrucache.hpp
// - Author:     Alexander Ponomarev
// - Created:    June 20, 2013, 5:09 PM
// - License:    BSD 3-Clause
//
// The following extensive changes have been applied to the original source:
//  * Cosmetic refactoring to fit the Tenzir code style
//  * Use of exceptions has been removed
//  * A `factory` member was added to create new entries if a key is
//    missing from the cache.
//  * Iteration support
//  * Member function `exists()` was renamed to `contains()`
//  * Added member function `resize()`
//  * Added member function `clear()`
//  * Added member function `eject()`

#pragma once

#include <cstddef>
#include <list>
#include <stdexcept>
#include <unordered_map>
#include <utility>

namespace tenzir::detail {

template <typename Key, typename Value, typename Factory>
class lru_cache {
public:
  using key_value_pair = std::pair<Key, Value>;
  using list_iterator = typename std::list<key_value_pair>::iterator;
  using const_list_iterator =
    typename std::list<key_value_pair>::const_iterator;

  lru_cache(size_t max_size, Factory factory)
    : max_size_(max_size), factory_(std::move(factory)) {
  }

  void clear() {
    cache_items_map_.clear();
    cache_items_list_.clear();
  }

  void resize(size_t max_size) {
    // FIXME: write unit test for this
    if (cache_items_list_.size() > max_size) {
      auto new_end = std::next(cache_items_list_.begin(), max_size);
      while (new_end != cache_items_list_.end()) {
        cache_items_map_.erase(new_end->first);
        auto position = new_end++;
        cache_items_list_.erase(position);
      }
    }
    max_size_ = max_size;
  }

  list_iterator begin() {
    return cache_items_list_.begin();
  }

  const_list_iterator begin() const {
    return cache_items_list_.begin();
  }

  list_iterator end() {
    return cache_items_list_.end();
  }

  const_list_iterator end() const {
    return cache_items_list_.end();
  }

  const Value& put(Key key, Value value) {
    auto it = cache_items_map_.find(key);
    cache_items_list_.push_front(
      key_value_pair(std::move(key), std::move(value)));
    if (it != cache_items_map_.end()) {
      cache_items_list_.erase(it->second);
      cache_items_map_.erase(it);
    }
    auto& result = cache_items_list_.begin()->second;
    cache_items_map_[key] = cache_items_list_.begin();
    if (cache_items_map_.size() > max_size_) {
      auto last = cache_items_list_.end();
      last--;
      cache_items_map_.erase(last->first);
      cache_items_list_.pop_back();
    }
    return result;
  }

  const Value& get_or_load(const Key& key) {
    auto it = cache_items_map_.find(key);
    if (it != cache_items_map_.end()) {
      cache_items_list_.splice(cache_items_list_.begin(), cache_items_list_,
                               it->second);
      return it->second->second;
    }
    return put(key, factory_(key));
  }

  void drop(const Key& key) {
    auto it = cache_items_map_.find(key);
    if (it != cache_items_map_.end()) {
      cache_items_list_.erase(it->second);
      cache_items_map_.erase(it);
    }
  }

  // Remove an item from the cache and return it; constructing
  // it if it didn't exist before.
  Value eject(const Key& key) {
    auto it = cache_items_map_.find(key);
    if (it != cache_items_map_.end()) {
      std::list<key_value_pair> tmp;
      tmp.splice(tmp.end(), cache_items_list_, it->second);
      cache_items_map_.erase(it);
      return std::move(tmp.front().second);
    } else {
      return factory_(key);
    }
  }

  bool contains(const Key& key) const {
    return cache_items_map_.find(key) != cache_items_map_.end();
  }

  [[nodiscard]] size_t size() const {
    return cache_items_map_.size();
  }

  Factory& factory() {
    return factory_;
  }

private:
  std::list<key_value_pair> cache_items_list_;
  std::unordered_map<Key, list_iterator> cache_items_map_;
  size_t max_size_;
  Factory factory_;
};

} // namespace tenzir::detail

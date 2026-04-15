//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
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
// The implementation below is adapted for Tenzir's coding style and APIs.
// See the git history for detailed changes.

#pragma once

#include "tenzir/detail/assert.hpp"

#include <cstddef>
#include <list>
#include <memory>
#include <unordered_map>
#include <utility>

namespace tenzir::detail {

template <class Key, class Value, class Factory>
class lru_cache {
public:
  using key_value_pair = std::pair<Key, Value>;
  using list_type = std::list<key_value_pair>;
  using list_iterator = typename list_type::iterator;
  using const_list_iterator = typename list_type::const_iterator;

  lru_cache(size_t max_size, Factory factory)
    : max_size_{max_size},
      factory_{std::move(factory)} {
  }

  auto clear() -> void {
    cache_items_map_.clear();
    cache_items_list_.clear();
    uncached_value_.reset();
  }

  auto resize(size_t max_size) -> void {
    while (cache_items_list_.size() > max_size) {
      auto last = std::prev(cache_items_list_.end());
      cache_items_map_.erase(last->first);
      cache_items_list_.erase(last);
    }
    max_size_ = max_size;
    uncached_value_.reset();
  }

  auto begin() -> list_iterator {
    return cache_items_list_.begin();
  }

  auto begin() const -> const_list_iterator {
    return cache_items_list_.begin();
  }

  auto end() -> list_iterator {
    return cache_items_list_.end();
  }

  auto end() const -> const_list_iterator {
    return cache_items_list_.end();
  }

  auto put(Key key, Value value) -> Value& {
    if (max_size_ == 0) {
      uncached_value_ = std::make_unique<Value>(std::move(value));
      return *uncached_value_;
    }
    if (auto it = cache_items_map_.find(key); it != cache_items_map_.end()) {
      cache_items_list_.erase(it->second);
      cache_items_map_.erase(it);
    }
    cache_items_list_.emplace_front(std::move(key), std::move(value));
    auto list_it = cache_items_list_.begin();
    auto inserted = cache_items_map_.emplace(list_it->first, list_it).second;
    TENZIR_ASSERT(inserted);
    if (cache_items_map_.size() > max_size_) {
      auto last = std::prev(cache_items_list_.end());
      cache_items_map_.erase(last->first);
      cache_items_list_.pop_back();
    }
    return list_it->second;
  }

  auto get(Key const& key) -> Value* {
    auto it = cache_items_map_.find(key);
    if (it == cache_items_map_.end()) {
      return nullptr;
    }
    cache_items_list_.splice(cache_items_list_.begin(), cache_items_list_,
                             it->second);
    return std::addressof(it->second->second);
  }

  auto get_or_load(Key const& key) -> Value& {
    if (auto value = get(key)) {
      return *value;
    }
    if (max_size_ == 0) {
      uncached_value_ = std::make_unique<Value>(factory_(key));
      return *uncached_value_;
    }
    return put(key, factory_(key));
  }

  auto peek(Key const& key) -> Value* {
    auto it = cache_items_map_.find(key);
    if (it == cache_items_map_.end()) {
      return nullptr;
    }
    return std::addressof(it->second->second);
  }

  auto peek(Key const& key) const -> Value const* {
    auto it = cache_items_map_.find(key);
    if (it == cache_items_map_.end()) {
      return nullptr;
    }
    return std::addressof(it->second->second);
  }

  auto drop(Key const& key) -> void {
    auto it = cache_items_map_.find(key);
    if (it != cache_items_map_.end()) {
      cache_items_list_.erase(it->second);
      cache_items_map_.erase(it);
    }
  }

  /// Remove an item from the cache and return it; constructing it if it did
  /// not exist before.
  auto eject(Key const& key) -> Value {
    auto it = cache_items_map_.find(key);
    if (it != cache_items_map_.end()) {
      auto result = std::move(it->second->second);
      cache_items_list_.erase(it->second);
      cache_items_map_.erase(it);
      return result;
    }
    return factory_(key);
  }

  [[nodiscard]] auto contains(Key const& key) const -> bool {
    return cache_items_map_.find(key) != cache_items_map_.end();
  }

  [[nodiscard]] auto size() const -> size_t {
    return cache_items_map_.size();
  }

  auto factory() -> Factory& {
    return factory_;
  }

private:
  list_type cache_items_list_;
  std::unordered_map<Key, list_iterator> cache_items_map_;
  size_t max_size_;
  Factory factory_;
  std::unique_ptr<Value> uncached_value_;
};

} // namespace tenzir::detail

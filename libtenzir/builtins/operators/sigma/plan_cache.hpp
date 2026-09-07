//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/detail/assert.hpp>

#include <cstddef>
#include <cstdint>
#include <iterator>
#include <list>
#include <unordered_map>
#include <utility>

namespace tenzir::plugins::sigma {

/// An LRU cache bounded by a cost budget rather than an entry count.
///
/// The sigma operator compiles its complete rule corpus once per distinct
/// schema shape, so every cached plan costs memory proportional to the
/// corpus. Bounding the entry count lets peak memory grow with the corpus
/// times the schema variety of the input; bounding the summed cost keeps it
/// proportional to the corpus alone. Insertion evicts the least recently used
/// entries until the budget holds, except that the entry just inserted always
/// stays resident: the plan for the slice in flight must exist even when it
/// alone exceeds the budget.
template <class Key, class Value>
class BudgetedLruCache {
public:
  explicit BudgetedLruCache(uint64_t budget) : budget_{budget} {
  }

  /// Returns the value for a key and marks it most recently used.
  auto get(Key const& key) -> Value* {
    auto const it = index_.find(key);
    if (it == index_.end()) {
      return nullptr;
    }
    entries_.splice(entries_.begin(), entries_, it->second);
    return std::addressof(it->second->value);
  }

  /// Inserts or replaces an entry as most recently used, then evicts the
  /// least recently used entries until the budget holds.
  auto put(Key key, Value value, uint64_t cost) -> Value& {
    if (auto const it = index_.find(key); it != index_.end()) {
      cost_ -= it->second->cost;
      entries_.erase(it->second);
      index_.erase(it);
    }
    entries_.push_front(Entry{std::move(key), std::move(value), cost});
    auto const inserted
      = index_.emplace(entries_.front().key, entries_.begin());
    TENZIR_ASSERT(inserted.second);
    cost_ += cost;
    evict();
    return entries_.front().value;
  }

  /// Replaces the budget and evicts until it holds.
  auto budget(uint64_t budget) -> void {
    budget_ = budget;
    evict();
  }

  auto clear() -> void {
    entries_.clear();
    index_.clear();
    cost_ = 0;
  }

  auto budget() const -> uint64_t {
    return budget_;
  }

  auto size() const -> size_t {
    return entries_.size();
  }

  /// The summed cost of the resident entries.
  auto cost() const -> uint64_t {
    return cost_;
  }

private:
  struct Entry {
    Key key;
    Value value;
    uint64_t cost;
  };

  using List = std::list<Entry>;

  /// Evicts from the least recently used end while the budget is exceeded,
  /// never touching the most recently used entry.
  auto evict() -> void {
    while (entries_.size() > 1 and cost_ > budget_) {
      auto const last = std::prev(entries_.end());
      cost_ -= last->cost;
      index_.erase(last->key);
      entries_.erase(last);
    }
  }

  List entries_;
  std::unordered_map<Key, typename List::iterator> index_;
  uint64_t budget_;
  uint64_t cost_ = 0;
};

} // namespace tenzir::plugins::sigma

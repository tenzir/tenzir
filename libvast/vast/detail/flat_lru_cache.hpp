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

#include <algorithm>
#include <type_traits>
#include <vector>

namespace vast::detail {

/// A flat LRU cache for elements that have a key-like member.
template <class T, class Predicate, class Factory>
class flat_lru_cache {
public:
  // -- member types -----------------------------------------------------------

  using vector_type = std::vector<T>;

  // -- constructors, destructors, and assignment operators -------------------

  flat_lru_cache(size_t size, Predicate pred = Predicate{},
                 Factory fac = Factory{})
    : size_(size),
      pred_(std::move(pred)),
      make_(std::move(fac)) {
    elements_.reserve(size_);
  }

  flat_lru_cache(flat_lru_cache&&) = default;

  flat_lru_cache& operator=(flat_lru_cache&&) = default;

  // -- properties -------------------------------------------------------------

  /// Queries whether `key` is present in the cache.
  template <class K>
  bool contains(const K& key) {
    auto first = elements_.begin();
    auto last = elements_.end();
    auto i = std::find_if(first, last, pred_(key));
    return i != last;
  }

  /// Gets the element matching the predicate or creates a new one.
  template <class K>
  T& get_or_add(const K& key) {
    auto first = elements_.begin();
    auto last = elements_.end();
    if (auto i = std::find_if(first, last, pred_(key)); i != last) {
      // Move to the back unless we already access the newest element.
      if (i != last - 1)
        std::rotate(i, i + 1, last);
      return elements_.back();
    }
    return add(make_(key));
  }

  /// @pre The new value's key does not collide with any existing element.
  T& add(T value) {
    // Fill cache if we didn't reach capacity yet.
    if (elements_.size() < size_)
      return elements_.emplace_back(std::move(value));
    // Evict oldest element by overriding it.
    auto first = elements_.begin();
    auto last = elements_.end();
    std::rotate(first, first + 1, last);
    return elements_.back() = std::move(value);
  }

  vector_type& elements() {
    return elements_;
  }

  const vector_type& elements() const {
    return elements_;
  }

  void size(size_t new_size) {
    if (new_size < elements_.size()) {
      auto first = elements_.begin();
      elements_.erase(first, first + (elements_.size() - new_size));
    } else {
      elements_.reserve(size_);
    }
    size_ = new_size;
  }

  size_t size() const noexcept {
    return size_;
  }

private:
  // -- member variables -------------------------------------------------------

  /// Flat store for elements. New elements are at the back, old elements are
  /// evicted from the front.
  vector_type elements_;

  /// Maximum number of elements.
  size_t size_;

  /// Implements key lookups for `T`.
  Predicate pred_;

  /// Creates new instances of `T`.
  Factory make_;
};

} // namespace vast::detail

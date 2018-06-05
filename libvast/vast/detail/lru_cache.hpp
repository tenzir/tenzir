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
class lru_cache {
public:
  // -- sanity checks ----------------------------------------------------------

  static_assert(std::is_empty_v<Predicate> && std::is_empty_v<Factory>);

  // -- member types -----------------------------------------------------------

  using vector_type = std::vector<T>;

  // -- constructors, destructors, and assignment operators -------------------

  lru_cache(size_t size) : size_(size) {
    elements_.reserve(size_);
  }

  lru_cache(lru_cache&&) = default;

  lru_cache& operator=(lru_cache&&) = default;

  // -- properties -------------------------------------------------------------

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
    // Fill cache if we didn't reach capacity yet.
    if (elements_.size() < size_)
      return elements_.emplace_back(make_(key));
    // Evict oldest element by overriding it.
    std::rotate(first, first + 1, last);
    return elements_.back() = make_(key);
  }

  vector_type& elements() {
    return elements_;
  }

private:
  // -- member variables -------------------------------------------------------

  /// Flat store for elements. New elements are at the back, old elements are
  /// evicted from the front.
  vector_type elements_;

  // Makes sure pred_ and make_ don't waste any space.
  union {
    /// Maximum number of elements.
    size_t size_;

    /// Implements key lookups for `T`.
    Predicate pred_;

    /// Creates new instances of `T`.
    Factory make_;
  };
};

} // namespace vast::detail

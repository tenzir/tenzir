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
#include <vector>

#include "vast/detail/operators.hpp"

namespace vast::detail {

/// A set abstraction as sorted STL vector.
template <class T, class Allocator, class Policy>
class vector_set : totally_ordered<vector_set<T, Allocator, Policy>> {
public:
  // -- types ----------------------------------------------------------------

  using value_type = T;
  using vector_type = std::vector<T, Allocator>;
  using allocator_type = typename vector_type::allocator_type;
  using size_type = typename vector_type::size_type;
  using difference_type = typename vector_type::difference_type;
  using reference = typename vector_type::reference;
  using const_reference = typename vector_type::const_reference;
  using pointer = typename vector_type::pointer;
  using const_pointer = typename vector_type::const_pointer;
  using iterator = typename vector_type::iterator;
  using const_iterator = typename vector_type::const_iterator;
  using reverse_iterator = typename vector_type::reverse_iterator;
  using const_reverse_iterator = typename vector_type::const_reverse_iterator;

  // -- construction ---------------------------------------------------------

  vector_set() = default;

  vector_set(std::initializer_list<T> l) {
    reserve(l.size());
    for (auto& x : l)
      insert(x);
  }

  template <class InputIterator>
  vector_set(InputIterator first, InputIterator last) {
    insert(first, last);
  }

  // -- iterators ------------------------------------------------------------

  iterator begin() {
    return xs_.begin();
  }

  const_iterator begin() const {
    return xs_.begin();
  }

  iterator end() {
    return xs_.end();
  }

  const_iterator end() const {
    return xs_.end();
  }

  reverse_iterator rbegin() {
    return xs_.rbegin();
  }

  const_reverse_iterator rbegin() const {
    return xs_.rbegin();
  }

  reverse_iterator rend() {
    return xs_.rend();
  }

  const_reverse_iterator rend() const {
    return xs_.rend();
  }

  // -- capacity -------------------------------------------------------------

  bool empty() const {
    return xs_.empty();
  }

  size_type size() const {
    return xs_.size();
  }

  void reserve(size_type count) {
    xs_.reserve(count);
  }

  void shrink_to_fit() {
    xs_.shrink_to_fit();
  }

  // -- modifiers ------------------------------------------------------------

  void clear() {
    return xs_.clear();
  }

  std::pair<iterator, bool> insert(const value_type& x) {
    return Policy::add(xs_, x);
  };

  std::pair<iterator, bool> insert(value_type&& x) {
    return Policy::add(xs_, std::move(x));
  };

  iterator insert(const_iterator, value_type x) {
    // TODO: don't ignore hint.
    return insert(std::move(x)).first;
  };

  template <class InputIterator>
  void insert(InputIterator first, InputIterator last) {
    while (first != last)
      insert(*first++);
  }

  template <class... Ts>
  std::pair<iterator, bool> emplace(Ts&&... xs) {
    return insert(value_type(std::forward<Ts>(xs)...));
  }

  template <class... Ts>
  std::pair<iterator, bool> emplace_hint(const_iterator, Ts&&... xs) {
    // TODO: don't ignore hint.
    return emplace(std::forward<Ts>(xs)...);
  }

  iterator erase(const_iterator i) {
    return xs_.erase(i);
  }

  iterator erase(const_iterator first, const_iterator last) {
    return xs_.erase(first, last);
  }

  size_type erase(const value_type& x) {
    auto i = std::remove(begin(), end(), x);
    if (i == end())
      return 0;
    erase(i);
    return 1;
  }

  void swap(vector_set& other) {
    xs_.swap(other);
  }

  // -- lookup ---------------------------------------------------------------

  size_type count(const value_type& x) const {
    return find(x) == end() ? 0 : 1;
  }

  iterator find(const value_type& x) {
    return Policy::lookup(xs_, x);
  }

  const_iterator find(const value_type& x) const {
    return Policy::lookup(xs_, x);
  }

  // -- operators ------------------------------------------------------------

  friend bool operator<(const vector_set& x, const vector_set& y) {
    return x.xs_ < y.xs_;
  }

  friend bool operator==(const vector_set& x, const vector_set& y) {
    return x.xs_ == y.xs_;
  }

  explicit operator const vector_type() const {
    return xs_;
  }

  template <class Inspector>
  friend auto inspect(Inspector&f, vector_set& xs) {
    return f(xs.xs_);
  }

private:
  vector_type xs_;
};

} // namespace vast::detail

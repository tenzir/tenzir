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
#include <functional>
#include <stdexcept>
#include <vector>

#include "vast/detail/operators.hpp"

namespace vast::detail {

/// A map abstraction over an unsorted `std::vector`.
template <class Key, class T, class Allocator, class Policy>
class vector_map : totally_ordered<vector_map<Key, T, Allocator, Policy>> {
public:
  // -- types ----------------------------------------------------------------

  using key_type = Key;
  using mapped_type = T;
  using value_type = std::pair<Key, T>;
  using vector_type = std::vector<value_type, Allocator>;
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

  vector_map() = default;

  vector_map(std::initializer_list<value_type> l) {
    reserve(l.size());
    for (auto& x : l)
      insert(x);
  }

  template <class InputIterator>
  vector_map(InputIterator first, InputIterator last) {
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

  std::pair<iterator, bool> insert(iterator, value_type x) {
    // TODO: don't ignore hint.
    return insert(std::move(x));
  };

  std::pair<iterator, bool> insert(const_iterator, value_type x) {
    // TODO: don't ignore hint.
    return insert(std::move(x));
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
    return emplace(std::forward<Ts>(xs)...);
  }

  iterator erase(const_iterator i) {
    return xs_.erase(i);
  }

  iterator erase(const_iterator first, const_iterator last) {
    return xs_.erase(first, last);
  }

  size_type erase(const key_type& x) {
    auto pred = [&](auto& p) { return p.first == x; };
    auto i = std::remove_if(begin(), end(), pred);
    if (i == end())
      return 0;
    erase(i);
    return 1;
  }

  void swap(vector_map& other) {
    xs_.swap(other);
  }

  // -- lookup ---------------------------------------------------------------

  mapped_type& at(const key_type& key) {
    auto i = find(key);
    if (i == end())
      throw std::out_of_range{"vast::detail::vector_map::at"};
    return i->second;
  }

  const mapped_type& at(const key_type& key) const {
    auto i = find(key);
    if (i == end())
      throw std::out_of_range{"vast::detail::vector_map::at"};
    return i->second;
  }

  mapped_type& operator[](const key_type& key) {
    auto i = find(key);
    if (i != end())
      return i->second;
    return xs_.insert(i, value_type{key, mapped_type{}})->second;
  }

  iterator find(const key_type& x) {
    return Policy::lookup(xs_, x);
  }

  const_iterator find(const key_type& x) const {
    return Policy::lookup(xs_, x);
  }

  size_type count(const key_type& x) const {
    return find(x) == end() ? 0 : 1;
  }

  // -- operators ------------------------------------------------------------

  friend bool operator<(const vector_map& x, const vector_map& y) {
    return x.xs_ < y.xs_;
  }

  friend bool operator==(const vector_map& x, const vector_map& y) {
    return x.xs_ == y.xs_;
  }

  explicit operator const vector_type() const {
    return xs_;
  }

private:
  vector_type xs_;
};

} // namespace vast::detail


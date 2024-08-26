//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2017 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/detail/operators.hpp"
#include "tenzir/detail/raise_error.hpp"

#include <algorithm>
#include <concepts>
#include <functional>
#include <type_traits>
#include <vector>

namespace tenzir::detail {

template <typename Policy, typename Key, typename Value>
concept vector_map_policy
  = requires(Policy p, Key k, Value v, std::vector<std::pair<Key, Value>> x,
             std::pair<Key, Value> kvp) {
      {
        p.add(x, k, v)
      } -> std::same_as<std::pair<typename decltype(x)::iterator, bool>>;
      { p.lookup(x, k) } -> std::same_as<typename decltype(x)::iterator>;
    };

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
    for (auto& x : l) {
      insert(x);
    }
  }

  template <class InputIterator>
  vector_map(InputIterator first, InputIterator last) {
    insert(first, last);
  }

  // -- iterators ------------------------------------------------------------

  iterator begin() {
    return xs_.begin();
  }

  [[nodiscard]] const_iterator begin() const {
    return xs_.begin();
  }

  iterator end() {
    return xs_.end();
  }

  [[nodiscard]] const_iterator end() const {
    return xs_.end();
  }

  reverse_iterator rbegin() {
    return xs_.rbegin();
  }

  [[nodiscard]] const_reverse_iterator rbegin() const {
    return xs_.rbegin();
  }

  reverse_iterator rend() {
    return xs_.rend();
  }

  [[nodiscard]] const_reverse_iterator rend() const {
    return xs_.rend();
  }

  // -- capacity -------------------------------------------------------------

  [[nodiscard]] bool empty() const {
    return xs_.empty();
  }

  [[nodiscard]] size_type size() const {
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

  template <typename Key_Like, typename... Args>
  std::pair<iterator, bool> try_emplace(Key_Like&& key, Args&&... args) {
    return Policy::try_emplace(xs_, std::forward<Key_Like>(key),
                               std::forward<Args>(args)...);
  }

  iterator insert(iterator, value_type x) {
    // TODO: don't ignore hint.
    return insert(std::move(x)).first;
  };

  iterator insert(const_iterator, value_type x) {
    // TODO: don't ignore hint.
    return insert(std::move(x)).first;
  };

  template <class InputIterator>
  void insert(InputIterator first, InputIterator last) {
    while (first != last) {
      insert(*first++);
    }
  }

  template <class... Ts>
  std::pair<iterator, bool> emplace(Ts&&... xs) {
    return insert(value_type(std::forward<Ts>(xs)...));
  }

  template <class... Ts>
  iterator emplace_hint(const_iterator, Ts&&... xs) {
    return emplace(std::forward<Ts>(xs)...).first;
  }

  iterator erase(const_iterator i) {
    return xs_.erase(i);
  }

  iterator erase(const_iterator first, const_iterator last) {
    return xs_.erase(first, last);
  }

  template <typename Key_Like>
    requires ( not ( 
        std::same_as<Key_Like, std::remove_cvref_t<iterator>> or
        std::same_as<Key_Like, std::remove_cvref_t<const_iterator>>
      ) )
  size_type erase(const Key_Like& x) {
    const auto it = find(x);
    if (it == end()) {
      return 0;
    }
    erase(it);
    return 1;
  }

  // size_type erase(const key_type& x) {
  //   auto pred = [&](auto& p) {
  //     return p.first == x;
  //   };
  //   auto i = std::remove_if(begin(), end(), pred);
  //   if (i == end()) {
  //     return 0;
  //   }
  //   erase(i);
  //   return 1;
  // }

  void swap(vector_map& other) {
    xs_.swap(other);
  }

  // -- lookup ---------------------------------------------------------------

  template <class L>
  [[nodiscard]] mapped_type& at(const L& key) {
    auto i = find(key);
    if (i == end()) {
      TENZIR_RAISE_ERROR(std::out_of_range,
                         "tenzir::detail::vector_map::at out of range");
    }
    return i->second;
  }

  template <class L>
  [[nodiscard]] const mapped_type& at(const L& key) const {
    auto i = find(key);
    if (i == end()) {
      TENZIR_RAISE_ERROR(std::out_of_range,
                         "tenzir::detail::vector_map::at out of range");
    }
    return i->second;
  }

  template <class L>
  [[nodiscard]] mapped_type& operator[](const L& key) {
    auto i = find(key);
    if (i != end()) {
      return i->second;
    }
    return insert(i, value_type{key, mapped_type{}})->second;
  }

  template <class L>
  [[nodiscard]] iterator find(const L& x) {
    return Policy::lookup(xs_, x);
  }

  template <class L>
  [[nodiscard]] const_iterator find(const L& x) const {
    return Policy::lookup(xs_, x);
  }

  template <class L>
  [[nodiscard]] size_type count(const L& x) const {
    return contains(x) ? 1 : 0;
  }

  template <class L>
  [[nodiscard]] bool contains(const L& x) const {
    return find(x) != end();
  }

  // -- operators ------------------------------------------------------------

  friend bool operator<(const vector_map& x, const vector_map& y) {
    return x.xs_ < y.xs_;
  }

  friend bool operator==(const vector_map& x, const vector_map& y) {
    return x.xs_ == y.xs_;
  }

  // -- non-standard API -----------------------------------------------------

  template <class Inspector>
  friend auto inspect(Inspector& f, vector_map& xs) {
    return f.apply(xs.xs_);
  }

  friend const vector_type& as_vector(const vector_map& xs) {
    return xs.xs_;
  }

  // A factory that takes a regular vector of pairs and converts it to a
  // vector_map. Explicitly unsafe because it can be used to produce a map with
  // multiple entries for the same key, which breaks normal map semantics.
  // This only exists to allow records of the form <1, 2, 3>, for which all
  // field names are empty strings.
  // TODO: Consider intruducing a tuple type in tenzir::data instead.
  static vector_map make_unsafe(vector_type xs) {
    return vector_map{std::move(xs)};
  }

private:
  explicit vector_map(vector_type&& xs) : xs_{std::move(xs)} {
    // nop
  }

  vector_type xs_;
};

} // namespace tenzir::detail

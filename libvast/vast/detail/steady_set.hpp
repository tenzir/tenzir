#ifndef VAST_DETAIL_STEADY_SET_HPP
#define VAST_DETAIL_STEADY_SET_HPP

#include <algorithm>
#include <functional>
#include <vector>

#include "vast/detail/operators.hpp"

namespace vast {
namespace detail {

/// A set abstraction over an unsorted `std::vector`.
template <class T, class Allocator = std::allocator<T>>
class steady_set : totally_ordered<steady_set<T, Allocator>> {
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

  steady_set() = default;

  steady_set(std::initializer_list<T> l) {
    reserve(l.size());
    for (auto& x : l)
      insert(x);
  }

  template <class InputIterator>
  steady_set(InputIterator first, InputIterator last) {
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

  std::pair<iterator, bool> insert(value_type x) {
    auto i = find(x);
    if (i == end())
      return {xs_.insert(i, std::move(x)), true};
    else
      return {i, false};
  };

  std::pair<iterator, bool> insert(iterator, value_type x) {
    return insert(std::move(x));
  };

  std::pair<iterator, bool> insert(const_iterator, value_type x) {
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

  size_type erase(const value_type& x) {
    auto i = std::remove(begin(), end(), x);
    if (i == end())
      return 0;
    erase(i);
    return 1;
  }

  void swap(steady_set& other) {
    xs_.swap(other);
  }

  // -- lookup ---------------------------------------------------------------

  size_type count(const value_type& x) const {
    return find(x) == end() ? 0 : 1;
  }

  iterator find(const value_type& x) {
    return std::find(begin(), end(), x);
  }

  const_iterator find(const value_type& x) const {
    return std::find(begin(), end(), x);
  }

  // -- operators ------------------------------------------------------------

  friend bool operator<(const steady_set& x, const steady_set& y) {
    return x.xs_ < y.xs_;
  }

  friend bool operator==(const steady_set& x, const steady_set& y) {
    return x.xs_ == y.xs_;
  }

  explicit operator const vector_type() const {
    return xs_;
  }

private:
  vector_type xs_;
};

} // namespace detail
} // namespace vast

#endif


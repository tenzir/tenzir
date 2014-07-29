#ifndef VAST_UTIL_FLAT_SET_H
#define VAST_UTIL_FLAT_SET_H

#include <vector>
#include "vast/util/operators.h"

namespace vast {
namespace util {

/// A set abstraction as sorted STL vector.
template <
  typename T,
  typename Compare = std::less<T>,
  typename Allocator = std::allocator<T>
>
class flat_set : totally_ordered<flat_set<T, Compare, Allocator>>
{
  using vector_type = std::vector<T, Allocator>;

  friend bool operator<(flat_set const& x, flat_set const& y)
  {
    return x.v_ < y.v_;
  }

  friend bool operator==(flat_set const& x, flat_set const& y)
  {
    return x.v_ == y.v_;
  }

public:
  //
  // Types
  //

  using value_type = typename vector_type::value_type;
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

  using compare = Compare;

  //
  // Construction
  //

  flat_set() = default;

  flat_set(std::initializer_list<T> l)
  {
    for (auto& x : l)
      insert(x);
  }

  template <typename InputIterator>
  flat_set(InputIterator first, InputIterator last)
  {
    insert(first, last);
  }

  //
  // Element access and lookup
  //

  reference at(size_type i)
  {
    return v_.at(i);
  }

  const_reference at(size_type i) const
  {
    return v_.at(i);
  }

  reference operator[](size_type i)
  {
    return v_[i];
  }

  const_reference operator[](size_type i) const
  {
    return v_[i];
  }

  reference front()
  {
    return v_.front();
  }

  const_reference front() const
  {
    return v_.front();
  }

  reference back()
  {
    return v_.back();
  }

  const_reference back() const
  {
    return v_.back();
  }

  value_type* data()
  {
    return v_.data();
  }

  value_type const* data() const
  {
    return v_.data();
  }

  iterator find(value_type const& x)
  {
    auto i = std::lower_bound(begin(), end(), x, compare{});
    return i == end() || compare{}(x, *i) ? end() : i;
  }

  const_iterator find(value_type const& x) const
  {
    auto i = std::lower_bound(begin(), end(), x, compare{});
    return i == end() || compare{}(x, *i) ? end() : i;
  }

  bool contains(value_type const& x) const
  {
    return find(x) != end();
  }

  //
  // Iterators
  //

  iterator begin()
  {
    return v_.begin();
  }

  const_iterator begin() const
  {
    return v_.begin();
  }

  iterator end()
  {
    return v_.end();
  }

  const_iterator end() const
  {
    return v_.end();
  }

  reverse_iterator rbegin()
  {
    return v_.rbegin();
  }

  const_reverse_iterator rbegin() const
  {
    return v_.rbegin();
  }

  reverse_iterator rend()
  {
    return v_.rend();
  }

  const_reverse_iterator rend() const
  {
    return v_.rend();
  }

  //
  // Capacity
  //

  bool empty() const
  {
    return v_.empty();
  }

  size_type size() const
  {
    return v_.size();
  }

  void reserve(size_type capacity)
  {
    v_.reserve(capacity);
  }

  void shrink_to_fit()
  {
    v_.shrink_to_fit();
  }

  //
  // Modifiers
  //

  void clear()
  {
    return v_.clear();
  }

  std::pair<iterator, bool> insert(T x)
  {
    auto i = std::lower_bound(begin(), end(), x);
    if (i == end() || compare{}(x, *i))
      return {v_.insert(i, std::move(x)), true};
    else
      return {i, false};
  };

  template <typename InputIterator>
  bool insert(InputIterator first, InputIterator last)
  {
    bool all = true;
    while (first != last)
      if (! insert(*first++).second)
        all = false;

    return all;
  }

  template <typename... Args>
  std::pair<iterator, bool> emplace(Args&&... args)
  {
    return insert(T(std::forward<Args>(args)...));
  }

  iterator erase(const_iterator i)
  {
    v_.erase(i);
  }

  iterator erase(const_iterator first, const_iterator last)
  {
    v_.erase(first, last);
  }

  void pop_back()
  {
    v_.pop_back();
  }

  bool resize(size_type n)
  {
    if (n >= v_.size())
      return false;

    v_.resize(n);
    return true;
  }

  void swap(flat_set& other)
  {
    v_.swap(other);
  }

  //
  // Algorithms
  //

  flat_set intersect(flat_set const& other) const
  {
    flat_set r;
    std::set_intersection(begin(), end(),
                          other.begin(), other.end(),
                          std::back_inserter(r));
    return r;
  }

  flat_set unify(flat_set const& other) const
  {
    flat_set r;
    std::set_intersection(begin(), end(),
                          other.begin(), other.end(),
                          std::back_inserter(r));
    return r;
  }

private:
  vector_type v_;
};

} // namespace util
} // namespace vast

#endif

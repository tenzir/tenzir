#ifndef VAST_DETAIL_RANGE_HPP
#define VAST_DETAIL_RANGE_HPP

#include "vast/detail/assert.hpp"
#include "vast/detail/iterator.hpp"

namespace vast {
namespace detail {

template <typename Derived>
class range {
public:
  explicit operator bool() const {
    return !empty();
  }

  bool operator!() const {
    return empty();
  }

  bool empty() const {
    auto d = static_cast<Derived const*>(this);
    return d->begin() == d->end();
  }
};

template <typename Range>
class range_iterator
  : public iterator_facade<
      range_iterator<Range>,
      decltype(std::declval<Range>().dereference()),
      std::forward_iterator_tag
    > {
  friend Range;
  friend iterator_access;

public:
  range_iterator() : rng_{nullptr} {
  }

private:
  explicit range_iterator(Range& rng) : rng_{&rng} {
  }

  bool equals(range_iterator const&) const {
    return rng_->complete();
  }

  void increment() {
    VAST_ASSERT(rng_);
    VAST_ASSERT(!rng_->complete());
    rng_->increment();
  }

  decltype(auto) dereference() const {
    VAST_ASSERT(rng_);
    return rng_->dereference();
  }

  Range* rng_;
};

template <typename Derived>
class range_facade : public range<range_facade<Derived>> {
public:
  using iterator = range_iterator<range_facade<Derived>>;
  using const_iterator = iterator;

  iterator begin() const {
    return iterator{const_cast<range_facade&>(*this)};
  }

  iterator end() const {
    return iterator{};
  }

private:
  friend iterator;

  bool complete() const {
    return static_cast<Derived const*>(this)->done();
  }

  void increment() {
    static_cast<Derived*>(this)->next();
  }

  decltype(auto) dereference() const {
    return static_cast<Derived const*>(this)->get();
  }
};

template <typename ForwardIterator>
class iterator_range : public range<iterator_range<ForwardIterator>> {
public:
  template <typename Iterator>
  iterator_range(Iterator begin, Iterator end)
    : begin_{begin}, end_{end} {
  }

  ForwardIterator begin() const {
    return begin_;
  }

  ForwardIterator end() const {
    return end_;
  }

private:
  ForwardIterator begin_;
  ForwardIterator end_;
};

template <class Iterator>
auto make_iterator_range(Iterator begin, Iterator end) {
  return iterator_range<Iterator>(begin, end);
}

} // namespace detail
} // namespace vast

#endif

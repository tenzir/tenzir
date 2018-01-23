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

#ifndef VAST_DETAIL_RANGE_HPP
#define VAST_DETAIL_RANGE_HPP

#include "vast/config.hpp"

#include "vast/detail/assert.hpp"
#include "vast/detail/iterator.hpp"

namespace vast::detail {

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
      std::remove_reference_t<decltype(std::declval<Range>().dereference())>,
      std::forward_iterator_tag,
      decltype(std::declval<Range>().dereference())
    > {
  friend Range;
  friend iterator_access;

public:
  range_iterator() : rng_{nullptr} {
  }

private:
  explicit range_iterator(Range& rng) : rng_{&rng} {
  }

  bool equals(const range_iterator&) const {
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

// FIXME: Why doesn't GCC like the above friend declaration?
#ifdef VAST_GCC
public:
#endif
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

} // namespace vast::detail

#endif

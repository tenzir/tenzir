#ifndef VAST_UTIL_RANGE_H
#define VAST_UTIL_RANGE_H

#include "vast/util/iterator.h"

namespace vast {
namespace util {

template <typename Derived>
class range
{
  Derived& derived()
  {
    return static_cast<Derived&>(*this);
  }

  Derived const& derived() const
  {
    return static_cast<Derived const&>(*this);
  }

public:
  explicit operator bool() const
  {
    return ! empty();
  }

  bool operator!() const
  {
    return empty();
  }

  bool empty() const
  {
    return derived().begin() == derived().end();
  }
};

template <typename ForwardIterator>
class iterator_range : range<iterator_range<ForwardIterator>>
{
public:
  template <typename Iterator>
  iterator_range(Iterator begin, Iterator end)
    : begin_{begin}, end_{end}
  {
  }

  ForwardIterator begin() const
  {
    return begin_;
  }

  ForwardIterator end() const
  {
    return end_;
  }

private:
  ForwardIterator begin_;
  ForwardIterator end_;
};

template <typename Range>
class range_iterator
  : public iterator_facade<
      range_iterator<Range>,
      std::forward_iterator_tag,
      typename Range::state_type
    >
{
  friend Range;
  friend iterator_access;

public:
  range_iterator() = default;

private:
  explicit range_iterator(Range& rng)
    : rng_{&rng}
  {
  }

  bool equals(range_iterator const& other) const
  {
    return rng_ == other.rng_;
  }

  void increment()
  {
    assert(rng_);
    if (! rng_->increment())
      rng_ = nullptr;
  }

  typename Range::state_type& dereference() const
  {
    return rng_->dereference();
  }

  Range* rng_ = nullptr;
};

template <typename Derived, typename State>
class range_facade : public range<range_facade<Derived, State>>
{
public:
  using state_type = typename std::add_const<State>::type;

  using iterator = range_iterator<range_facade<Derived, State>>;
  using const_iterator = iterator;

  iterator begin() const
  {
    return iterator{const_cast<range_facade&>(*this)};
  }

  iterator end() const
  {
    return iterator{};
  }

private:
  friend iterator;

  bool increment()
  {
    return static_cast<Derived*>(this)->next();
  }

  state_type& dereference() const
  {
    return static_cast<Derived const*>(this)->state();
  }
};

} // namespace util
} // namespace vast

#endif

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

template <typename Range>
class range_iterator
  : public iterator_facade<
      range_iterator<Range>,
      std::forward_iterator_tag,
      decltype(std::declval<Range>().dereference())
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

  auto dereference() const
    -> decltype(std::declval<Range>().dereference())
  {
    return rng_->dereference();
  }

  Range* rng_ = nullptr;
};

template <typename Derived>
class range_facade : public range<range_facade<Derived>>
{
public:
  using iterator = range_iterator<range_facade<Derived>>;
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

  template <typename Hack = Derived>
  auto dereference() const
    -> decltype(std::declval<Hack>().state())
  {
    static_assert(std::is_same<Hack, Derived>::value, ":-P");
    return static_cast<Derived const*>(this)->state();
  }
};

template <typename ForwardIterator>
class iterator_range : public range<iterator_range<ForwardIterator>>
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

} // namespace util
} // namespace vast

#endif

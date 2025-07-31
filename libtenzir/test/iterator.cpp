//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/detail/iterator.hpp"

#include "tenzir/test/test.hpp"

using namespace tenzir;

// A simple iterator over an array.
template <class T, size_t N>
struct iterator
  : detail::iterator_facade<iterator<T, N>, T, std::random_access_iterator_tag> {
public:
  iterator() = default;

  iterator(T(&array)[N]) : array_{array}, i_{0} {
  }

private:
  friend detail::iterator_access;

  void increment() {
    ++i_;
  }

  void decrement() {
    --i_;
  }

  template <class Distance>
  void advance(Distance n) {
    i_ += n;
  }

  auto distance_to(const iterator& other) const {
    using distance = std::ptrdiff_t;
    return static_cast<distance>(other.i_) - static_cast<distance>(i_);
  }

  T& dereference() const {
    return *(array_ + i_);
  }

  bool equals(const iterator& other) const {
    return i_ == other.i_;
  }

  T* array_;
  size_t i_ = N;
};

TEST("basic_custom_iterator") {
  int a[5] = {1, 2, 3, 4, 5};
  iterator<int, 5> begin{a}, end;

  int i = 0;
  while (begin != end)
    CHECK(*begin++ == ++i);

  begin -= 3;
  CHECK(*begin == 3);
  *begin = 42;
  CHECK(*begin == 42);

  CHECK(*--begin == 2);
  CHECK((end - begin) == 4);
  CHECK((begin + 4) == end);

  CHECK(!(begin == end));
  CHECK(begin != end);
  CHECK(begin < end);
  CHECK(!(end < begin));
  CHECK(begin <= end);
}

TEST("basic_custom_const_iterator") {
  int a[5] = {1, 2, 3, 4, 5};
  iterator<const int, 5> begin{a}, end;

  int i = 0;
  while (begin != end)
    CHECK(*begin++ == ++i);
}

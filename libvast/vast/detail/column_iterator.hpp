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

#include <cstddef>
#include <iterator>

#include "vast/detail/narrow.hpp"

namespace vast::detail {

/// Iterates a 1-dimensional row-major array as if it were a column in a
/// 2-dimensional matrix.
template <class T>
class column_iterator : public std::iterator<std::random_access_iterator_tag, T,
                                             ptrdiff_t, T*, T&> {
public:
  // -- member types ---------------------------------------------------------

  using super = std::iterator<std::random_access_iterator_tag,
                              T, ptrdiff_t, T*, T&>;

  using difference_type = typename super::difference_type;

  using value_type = typename super::value_type;

  using pointer = typename super::pointer;

  using reference = typename super::reference;

  // -- constructors, destructors, and assignment operators ------------------

  column_iterator(pointer ptr, difference_type columns) : ptr_(ptr), columns_(columns) {
    //nop
  }

  column_iterator(pointer ptr, size_t columns)
    : column_iterator(ptr, narrow_cast<difference_type>(columns)) {
    //nop
  }

  column_iterator(const column_iterator&) = default;

  column_iterator& operator=(const column_iterator&) = default;

  // -- operators ------------------------------------------------------------

  bool operator==(const column_iterator& other) const noexcept {
    return ptr_ == other.ptr_;
  }

  bool operator!=(const column_iterator& other) const noexcept {
    return ptr_ != other.ptr_;
  }

  column_iterator& operator++() {
    ptr_ += columns_;
    return *this;
  }

  column_iterator& operator+=(difference_type n) {
    ptr_ += columns_ * n;
    return *this;
  }

  column_iterator operator+(difference_type n) const {
    auto result{*this};
    result += n;
    return result;
  }

  column_iterator operator++(int) {
    auto result = *this;
    ptr_ += columns_;
    return result;
  }

  column_iterator& operator--() {
    ptr_ -= columns_;
    return *this;
  }

  column_iterator& operator-=(difference_type n) {
    ptr_ -= columns_ * n;
    return *this;
  }

  column_iterator operator-(difference_type n) const {
    auto result{*this};
    result -= n;
    return result;
  }

  column_iterator operator--(int) {
    auto result = *this;
    ptr_ -= columns_;
    return result;
  }

  difference_type operator-(const column_iterator& other) {
    return (ptr_ - other.ptr_) / columns_;
  }

  reference operator[](difference_type pos) {
    return ptr_[pos * columns_];
  }

  reference operator*() {
    return *ptr_;
  }

  pointer operator->() {
    return ptr_;
  }

private:
  // -- member variables -----------------------------------------------------

  pointer ptr_;
  difference_type columns_;
};

} // namespace vast::detail

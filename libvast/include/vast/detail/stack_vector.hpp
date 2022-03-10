//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/detail/short_alloc.hpp"

#include <scoped_allocator>
#include <vector>

namespace vast::detail {

// This base class exists to bind the lifetime of the corresponding container
// to the arena, such that the allocator will never outlive the the stack-based
// container.
template <class T, size_t N>
struct stack_container {
  using allocator_type = short_alloc<T, N>;
  using arena_type = typename allocator_type::arena_type;

  arena_type arena_;
};

/// A std::vector operating with short_alloc as allocator.
template <class T, size_t N>
using short_vector =
  std::vector<T, std::scoped_allocator_adaptor<short_alloc<T, N>>>;

/// A stack-based vector.
/// @tparam T The element type of the vector.
/// @tparam N The number of bytes to be keep on the stack before moving to the
///           heap.
template <class T, size_t N>
struct stack_vector : private stack_container<T, N>, short_vector<T, N> {
  using vector_type = short_vector<T, N>;

  stack_vector() : vector_type(this->arena_) {
  }

  stack_vector(size_t n, const T& x) : vector_type(n, x, this->arena_) {
  }

  explicit stack_vector(size_t n) : vector_type(n, this->arena_) {
  }

  stack_vector(std::initializer_list<T> init)
    : vector_type(std::move(init), this->arena_) {
  }

  template <class Iterator>
  stack_vector(Iterator first, Iterator last)
    : vector_type(first, last, this->arena_) {
  }

  stack_vector(const stack_vector& other)
    : vector_type(other, this->arena_) {
  }

  stack_vector(stack_vector&& other)
  noexcept(std::is_nothrow_move_constructible_v<vector_type>)
    : vector_type(std::move(other), this->arena_) {
  }

  stack_vector& operator=(const stack_vector& other) {
    static_cast<vector_type&>(*this) = other;
    return *this;
  }

  stack_vector& operator=(stack_vector&& other)
  noexcept(std::is_nothrow_move_assignable_v<vector_type>) {
    static_cast<vector_type&>(*this) = std::move(other);
    return *this;
  }
};

} // namespace vast::detail


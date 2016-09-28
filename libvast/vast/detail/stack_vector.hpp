#ifndef VAST_DETAIL_STACK_VECTOR
#define VAST_DETAIL_STACK_VECTOR

#include <vector>
#include <scoped_allocator>

#include "vast/detail/short_alloc.hpp"

namespace vast {
namespace detail {

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

  stack_vector(size_t n, T const& x) : vector_type(n, x, this->arena_) {
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

  stack_vector(stack_vector const& other)
    : vector_type(other, this->arena_) {
  }

  stack_vector(stack_vector&& other)
  noexcept(std::is_nothrow_move_constructible<vector_type>::value)
    : vector_type(std::move(other), this->arena_) {
  }

  stack_vector& operator=(stack_vector const& other) {
    static_cast<vector_type&>(*this) = other;
    return *this;
  }

  stack_vector& operator=(stack_vector&& other)
  noexcept(std::is_nothrow_move_assignable<vector_type>::value) {
    static_cast<vector_type&>(*this) = std::move(other);
    return *this;
  }
};

} // namespace detail
} // namespace vast

#endif

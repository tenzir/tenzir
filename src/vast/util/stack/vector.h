#ifndef VAST_UTIL_STACK_VECTOR
#define VAST_UTIL_STACK_VECTOR

#include <vector>
#include <scoped_allocator>
#include "vast/util/stack/allocator.h"

namespace vast {
namespace util {
namespace stack {

/// A stack-based vector.
/// @tparam N The number of elements to be kept on the stack. This should be a
///           multiple of 2 because most implementations of std::vector grow
///           geometrically.
/// @tparam T The element type of the vector.
template <size_t N, typename T>
struct vector 
  : detail::container_base<N, T>,
    std::vector<T, std::scoped_allocator_adaptor<allocator<T, N>>>
{
  using vector_type =
    std::vector<T, std::scoped_allocator_adaptor<allocator<T, N>>>;

  using allocator_type =
    typename vector_type::allocator_type::outer_allocator_type;

  using arena_type = typename allocator_type::arena_type;

  vector()
    : vector_type(this->allocator)
  {
  }

  vector(size_t n, T const& x)
    : vector_type(n, x, this->allocator)
  {
  }

  explicit vector(size_t n)
    : vector_type(n, this->allocator)
  {
  }

  vector(std::initializer_list<T> init)
    : vector_type(std::move(init), this->allocator)
  {
  }

  template <class Iterator>
  vector(Iterator first, Iterator last)
    : vector_type(first, last, this->allocator)
  {
  }

  vector(vector const& other)
    : detail::container_base<N, T>::container_base(other),
      vector_type(other, this->allocator)
  {
  }

  vector(vector&& other) noexcept
    : vector_type(std::move(other), this->allocator)
  {
  }

  vector& operator=(vector const& other)
  {
    // FIXME: this is probably not optimal
    this->assign(other.begin(), other.end());
    return *this;
  }
};

} // namespace stack
} // namespace util
} // namespace vast

#endif

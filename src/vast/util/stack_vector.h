#ifndef VAST_UTIL_STACK_VECTOR
#define VAST_UTIL_STACK_VECTOR

#include <vector>
//#include "vast/util/stack_alloc.h"

namespace vast {
namespace util {

template <typename T, size_t>
using stack_vector = std::vector<T>;

// TODO: implement correctly.
//template <typename T, size_t N>
//class stack_vector : public stack_container<std::vector, T, N>
//{
//  using super = stack_container<std::vector, T, N>;
//
//public:
//  stack_vector()
//    : super(arena_)
//  {
//  }
//
//  stack_vector(size_t n, T const& x)
//    : super(n, x, arena_)
//  {
//  }
//
//  stack_vector(size_t n)
//    : super(n, arena_)
//  {
//  }
//
//  stack_vector(std::initializer_list<T> list)
//    : super(std::move(list), arena_)
//  {
//  }
//
//  template <typename Iterator>
//  stack_vector(Iterator begin, Iterator end)
//    : super(begin, end, arena_)
//  {
//  }
//
//  stack_vector(stack_vector const& other)
//    : super(other, arena_),
//      arena_(other.arena_)
//  {
//  }
//
//  stack_vector(stack_vector&& other) noexcept
//    : super(std::move(other), arena_),
//      arena_(std::move(other.arena_))
//  {
//  }
//
//  stack_vector& operator=(stack_vector const& other)
//  {
//    using std::swap;
//    stack_vector tmp(other);
//    swap(*this, tmp);
//    return *this;
//  }
//
//  stack_vector& operator=(stack_vector&& other) noexcept
//  {
//    using std::swap;
//    stack_vector tmp(std::move(other));
//    swap(*this, tmp);
//    return *this;
//  }
//
//private:
//  typename arena_alloc<T, N>::arena_type arena_;
//};

} // namespace util
} // namespace vast

#endif

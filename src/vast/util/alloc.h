#ifndef VAST_UTIL_ALLOC_H
#define VAST_UTIL_ALLOC_H

#include <cstddef>
#include <cassert>
#include "vast/util/operators.h"

namespace vast {
namespace util {

/// A fixed-size stack buffer for allocating/deallocating memory. When
/// requesting memory after it reached its capacity, the arena uses the free
/// store to retrieve further space.
/// @tparam N The number of bytes in the arena.
template <size_t N>
class arena : equality_comparable<arena<N>>
{
  static constexpr size_t alignment = 16;

public:
  arena() noexcept
    : ptr_{buf_}
  {
  }

  arena(arena const& other) noexcept
    : ptr_{buf_ + other.used()}
  {
    std::copy(other.buf_, other.buf_ + other.used(), buf_);
  }

  ~arena()
  {
    ptr_ = nullptr;
  }

  /// Allocates a chunk of bytes.
  /// @param n The number of bytes of the chunk to allocate.
  /// @returns A pointer to the allocated chunk.
  char* allocate(size_t n)
  {
    assert(pointer_in_buffer(ptr_) && "allocator has outlived arena");
    if (static_cast<size_t>(buf_ + N - ptr_) >= n)
    {
      auto r = ptr_;
      ptr_ += n;
      return r;
    }

    return static_cast<char*>(::operator new(n));
  }

  /// Deallocates a pointer.
  /// @param p The pointer to allocate.
  /// @param n The size of the chunk *p* points to.
  void deallocate(char* p, size_t n) noexcept
  {
    assert(pointer_in_buffer(ptr_) && "allocator has outlived arena");
    if (pointer_in_buffer(p))
    {
      if (p + n == ptr_)
        ptr_ = p;
    }
    else
    {
      ::operator delete(p);
    }
  }

  /// Retrieves the arena capacity.
  /// @returns The number of bytes the arena provides.
  static constexpr size_t size()
  {
    return N;
  }

  /// Retrieves the number of bytes used.
  /// @returns The number of bytes the arena uses.
  size_t used() const
  {
    return static_cast<size_t>(ptr_ - buf_);
  }

  /// Resets the arena.
  void reset()
  {
    ptr_ = buf_;
  }

private:
  friend bool operator==(arena const& x, arena const& y)
  {
    return std::equal(x.buf_, x.buf_ + N, y.buf_);
  }

  bool pointer_in_buffer(char* p) noexcept
  {
    return buf_ <= p && p <= buf_ + N;
  }

  alignas(alignment) char buf_[N];
  char* ptr_;
};


/// A stack-based allocator.
/// @see http://home.roadrunner.com/~hinnant/stack_alloc.html.
template <class T, size_t N>
class stack_alloc : equality_comparable<stack_alloc<T, N>>
{
  template <typename U, size_t M>
  friend class stack_alloc;

public:
  using value_type = T;

  template <class U>
  struct rebind
  {
    using other = stack_alloc<U, N>;
  };

  stack_alloc() = default;

  stack_alloc(stack_alloc const&) = default;

  template <typename U>
  stack_alloc(stack_alloc<U, N> const& other)
    : a_{other.a_}
  {
  }

  T* allocate(size_t n)
  {
    return reinterpret_cast<T*>(a_.allocate(n * sizeof(T)));
  }

  void deallocate(T* p, size_t n) noexcept
  {
    a_.deallocate(reinterpret_cast<char*>(p), n * sizeof(T));
  }

  const util::arena<N>& arena() const
  {
    return a_;
  }

private:
  friend bool operator==(stack_alloc const& x, stack_alloc const& y) noexcept
  {
    return x.a_ == y.a_;
  }

  util::arena<N> a_;
};

} // namespace util
} // namespace vast

#endif

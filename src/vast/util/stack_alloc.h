#ifndef VAST_UTIL_STACK_ALLOC_H
#define VAST_UTIL_STACK_ALLOC_H

#include <cstddef>
#include <cassert>
#include <scoped_allocator>
#include <type_traits>
#include "vast/util/operators.h"

namespace vast {
namespace util {

// Details:
// - http://bit.ly/allocator-user-guide
// - http://home.roadrunner.com/~hinnant/short_alloc.html.
// - http://home.roadrunner.com/~hinnant/stack_alloc.html.

/// A fixed-size stack buffer for allocating/deallocating memory. When
/// requesting memory after it reached its capacity, the arena uses the free
/// store to retrieve additional space.
/// @tparam N The number of bytes in the arena.
/// @tparam Align The alignment to use for the *N* bytes.
template <size_t N, size_t Align = 16>
class arena : equality_comparable<arena<N, Align>>
{
public:
  arena() noexcept
    : ptr_{buf_}
  {
  }

  ~arena()
  {
    ptr_ = nullptr;
  }

  arena(arena const& other) noexcept
    : ptr_{buf_ + other.used()}
  {
    std::copy(other.buf_, other.buf_ + N, buf_);
  }

  arena(arena&& other) noexcept
    : ptr_{buf_ + other.used()}
  {
    std::copy(other.buf_, other.buf_ + N, buf_);
  }

  arena& operator=(arena const& other) noexcept
  {
    std::copy(other.buf_, other.buf_ + N, buf_);
    ptr_ = buf_ + other.used();
    return *this;
  }

  arena& operator=(arena&& other) noexcept
  {
    std::copy(other.buf_, other.buf_ + N, buf_);
    ptr_ = buf_ + other.used();
    return *this;
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

  /// Deallocates a chunk of bytes.
  /// @param p A pointer to the beginning of the bytes to deallocate.
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
    return std::equal(x.buf_, x.buf_ + N, y.buf_, y.buf_ + N);
  }

  bool pointer_in_buffer(char* p) noexcept
  {
    return buf_ <= p && p <= buf_ + N;
  }

  alignas(Align) char buf_[N];
  char* ptr_;
};

/// An arena allocator which uses a given ::arena for allocation/deallocation.
/// @tparam T The type of the element to allocate.
/// @tparam N The number of elements the stack arena should be able to hold.
template <typename T, size_t N>
class arena_alloc
{
  template <typename, size_t>
  friend class arena_alloc;

public:
  using value_type = T;
  using arena_type = arena<N * sizeof(T), std::alignment_of<T>::value>;

  template <typename U>
  struct rebind
  {
    using other = arena_alloc<U, N>;
  };

  arena_alloc(arena_type& a) noexcept
    : arena_(a)
  {
  }

  template <typename U>
  arena_alloc(arena_alloc<U, N> const& a) noexcept
    : arena_(a.arena_)
  {
  }

  arena_alloc(arena_alloc const&) = default;
  arena_alloc& operator=(arena_alloc const&) = delete;

  T* allocate(size_t n)
  {
    return reinterpret_cast<T*>(arena_.allocate(n * sizeof(T)));
  }

  void deallocate(T* p, size_t n) noexcept
  {
    arena_.deallocate(reinterpret_cast<char*>(p), n * sizeof(T));
  }

  template <typename U, size_t M>
  friend bool
  operator==(arena_alloc const& x, arena_alloc<U, M> const& y) noexcept
  {
    // Two allocators are equal iff they share the same arena.
    return N == M && &x.arena_ == &y.arena_;
  }

  template <typename U, size_t M>
  friend bool
  operator!=(arena_alloc const& x, arena_alloc<U, M> const& y) noexcept
  {
    return ! (x == y);
  }

private:
  arena_type& arena_;
};

/// A stack-based container.
/// @tparam C The container to use with ::stack_alloc.
/// @tparam T The vector element type.
/// @tparam N The number of elements to keep on the stack.
template <template <typename...> class C, typename T, size_t N>
using stack_container = C<T, std::scoped_allocator_adaptor<arena_alloc<T, N>>>;

} // namespace util
} // namespace vast

#endif

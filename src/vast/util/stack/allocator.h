#ifndef VAST_UTIL_STACK_ALLOCATOR_H
#define VAST_UTIL_STACK_ALLOCATOR_H

#include <cstddef>
#include <cassert>
#include <type_traits>

#include "vast/config.h"
#include "vast/util/assert.h"
#include "vast/util/operators.h"

namespace vast {
namespace util {
namespace stack {

// Details:
// - http://bit.ly/allocator-user-guide
// - http://howardhinnant.github.io/short_alloc.html.
// - http://howardhinnant.github.io/stack_alloc.html.

/// A fixed-size stack buffer for allocating/deallocating memory. When
/// requesting memory after it reached its capacity, the arena uses the free
/// store to retrieve additional space.
/// @tparam N The number of bytes in the arena.
/// @tparam Align The alignment to use for the *N* bytes.
template <size_t N, size_t Align = 16>
class arena : equality_comparable<arena<N, Align>> {
public:
  arena() noexcept : ptr_{buf_} {
  }

  ~arena() {
    ptr_ = nullptr;
  }

  /// Copy-constructs the arena.
  arena(arena const& other) noexcept : ptr_{buf_ + other.used()} {
    std::copy(other.buf_, other.buf_ + N, buf_);
  }

  /// Allocates a chunk of bytes.
  /// @param n The number of bytes of the chunk to allocate.
  /// @returns A pointer to the allocated chunk.
  char* allocate(size_t n) {
    VAST_ASSERT(pointer_in_buffer(ptr_) && "allocator has outlived arena");
    if (static_cast<size_t>(buf_ + N - ptr_) >= n) {
      auto r = ptr_;
      ptr_ += n;
      return r;
    }

    return static_cast<char*>(::operator new(n));
  }

  /// Deallocates a chunk of bytes.
  /// @param p A pointer to the beginning of the bytes to deallocate.
  /// @param n The size of the chunk *p* points to.
  void deallocate(char* p, size_t n) noexcept {
    VAST_ASSERT(pointer_in_buffer(ptr_) && "allocator has outlived arena");
    if (!pointer_in_buffer(p))
      ::operator delete(p);
    else if (p + n == ptr_)
      ptr_ = p;
  }

  /// Retrieves the arena capacity.
  /// @returns The number of bytes the arena provides.
  static constexpr size_t size() {
    return N;
  }

  /// Retrieves the number of bytes used.
  /// @returns The number of bytes the arena uses.
  size_t used() const {
    return static_cast<size_t>(ptr_ - buf_);
  }

  /// Resets the arena.
  void reset() {
    ptr_ = buf_;
  }

  /// Retrieves the buffer for inspection.
  char const* data() {
    return buf_;
  }

private:
  friend bool operator==(arena const& x, arena const& y) {
    return std::equal(x.buf_, x.buf_ + N, y.buf_, y.buf_ + N);
  }

  bool pointer_in_buffer(char* p) noexcept {
    return buf_ <= p && p <= buf_ + N;
  }

  alignas(Align) char buf_[N];
  char* ptr_;
};

/// An allocator which uses a given ::arena as memory.
/// @tparam T The type of the element to allocate.
/// @tparam N The number of elements the stack arena should be able to hold.
template <typename T, size_t N>
class allocator {
  template <typename, size_t>
  friend class allocator;

public:
  using arena_type = arena<N * sizeof(T), std::alignment_of<T>::value>;

  using value_type = T;
  using pointer = T*;

  using propagate_on_container_copy_assignment = std::true_type;
  using propagate_on_container_move_assignment = std::true_type;
  using propagate_on_container_swap = std::true_type;

// This should be provided by std::allocator_traits, but buggy GCC 4.9
// complains.
#ifdef VAST_GCC
  template <typename U>
  struct rebind {
    using other = allocator<U, N>;
  };
#endif

  explicit allocator(arena_type* a) noexcept : arena_{a} {
  }

  template <typename U, size_t M>
  allocator(allocator<U, M> const& other) noexcept : arena_{other.arena_} {
  }

  pointer allocate(size_t n) {
    return reinterpret_cast<pointer>(arena_->allocate(n * sizeof(T)));
  }

  void deallocate(pointer p, size_t n) {
    arena_->deallocate(reinterpret_cast<char*>(p), n * sizeof(T));
  }

  // Some bug in GCC prevents the inline friend defintion of the operator, so
  // we just declare it here.
  template <typename U, size_t N0, typename V, size_t N1>
  friend bool operator==(allocator<U, N0> const& x, allocator<V, N1> const& y);

  template <typename U, size_t N0, typename V, size_t N1>
  friend bool operator!=(allocator<U, N0> const& x, allocator<V, N1> const& y);

private:
  arena_type* arena_;
};

template <typename U, size_t N0, typename V, size_t N1>
bool operator==(allocator<U, N0> const& x, allocator<V, N1> const& y) {
  return x.arena_ == y.arena_;
}

template <typename U, size_t N0, typename V, size_t N1>
bool operator!=(allocator<U, N0> const& x, allocator<V, N1> const& y) {
  return x.arena_ != y.arena_;
}

namespace detail {

// This base class exists so that the arena and allocator will always outlive
// the stack-based container, which inherits from both this class and the
// standard library container.
template <size_t N, typename T>
struct container_base {
  container_base() : allocator{&arena} {
  }

  container_base(container_base const& other)
    : arena{other.arena}, allocator{&arena} {
  }

  container_base& operator=(container_base const& other) = delete;
  container_base& operator=(container_base&& other) = delete;

  using allocator_type = stack::allocator<T, N>;
  using arena_type = typename allocator_type::arena_type;
  arena_type arena;
  allocator_type allocator;
};

} // namespace detail
} // namespace stack
} // namespace util
} // namespace vast

#endif

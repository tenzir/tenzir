// A stack-based allocator from Howard Hinnant.
// http://home.roadrunner.com/~hinnant/stack_alloc.html.
#ifndef VAST_UTIL_SHORT_ALLOC_H
#define VAST_UTIL_SHORT_ALLOC_H

#include <cstddef>
#include <cassert>

namespace vast {
namespace util {

template <size_t N>
class arena
{
  static constexpr size_t alignment = 16;

  arena(arena const&) = delete;
  arena& operator=(arena const&) = delete;

public:
  arena() noexcept
    : ptr_{buf_}
  {}

  ~arena()
  {
    ptr_ = nullptr;
  }

  char* allocate(size_t n)
  {
    assert(pointer_in_buffer(ptr_) && "short_alloc has outlived arena");
    if (static_cast<size_t>(buf_ + N - ptr_) >= n)
    {
      auto r = ptr_;
      ptr_ += n;
      return r;
    }

    return static_cast<char*>(::operator new(n));
  }

  void deallocate(char* p, size_t n) noexcept
  {
    assert(pointer_in_buffer(ptr_) && "short_alloc has outlived arena");
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

  static constexpr size_t size()
  {
    return N;
  }

  size_t used() const
  {
    return static_cast<size_t>(ptr_ - buf_);
  }

  void reset()
  {
    ptr_ = buf_;
  }

private:
  bool pointer_in_buffer(char* p) noexcept
  {
    return buf_ <= p && p <= buf_ + N;
  }

  alignas(alignment) char buf_[N];
  char* ptr_;
};


template <class T, size_t N>
class short_alloc
{
  template <class U, size_t M>
  friend class short_alloc;

public:
  using value_type = T;

  template <class _Up>
  struct rebind
  {
    using other = short_alloc<_Up, N>;
  };

  short_alloc(arena<N>& a) noexcept
    : a_{a}
  {}

  template <class U>
  short_alloc(short_alloc<U, N> const& a) noexcept
    : a_{a.a_}
  {}

  short_alloc(short_alloc const&) = default;
  short_alloc& operator=(short_alloc const&) = delete;

  T* allocate(size_t n)
  {
    return reinterpret_cast<T*>(a_.allocate(n*sizeof(T)));
  }

  void deallocate(T* p, size_t n) noexcept
  {
    a_.deallocate(reinterpret_cast<char*>(p), n*sizeof(T));
  }

  template <class T1, size_t N1, class U, size_t M>
  friend bool
  operator==(short_alloc<T1, N1> const& x, short_alloc<U, M> const& y) noexcept
  {
    return N == M && &x.a_ == &y.a_;
  }

  template <class T1, size_t N1, class U, size_t M>
  friend bool
  operator!=(short_alloc<T1, N1> const& x, short_alloc<U, M> const& y) noexcept
  {
    return ! (x == y);
  }

private:
  arena<N>& a_;
};

} // namespace util
} // namespace vast

#endif

// The MIT License (MIT)
//
// Copyright (c) 2015 Howard Hinnant
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
//
// Adapted to fit VAST's coding style.

#ifndef VAST_DETAIL_SHORT_ALLOC_HPP
#define VAST_DETAIL_SHORT_ALLOC_HPP

#include <cstddef>
#include <cassert>
#include <type_traits>

#include "vast/detail/assert.hpp"

namespace vast {
namespace detail {

// See the following resources for details:
// - http://howardhinnant.github.io/short_alloc.html.
// - http://bit.ly/allocator-user-guide

template <size_t N, size_t Alignment = alignof(std::max_align_t)>
class arena {
public:
  ~arena() {
    ptr_ = nullptr;
  }

  arena() noexcept : ptr_(buf_) {
  }

  arena(const arena&) = delete;
  arena& operator=(const arena&) = delete;

  template <size_t RequiredAlignment>
  char* allocate(size_t n) {
    static_assert(RequiredAlignment <= Alignment,
                  "alignment is too small for this arena");
    static_assert(Alignment <= alignof(std::max_align_t),
                  "you've chosen an alignment that is larger than "
                  "alignof(std::max_align_t), and cannot be guaranteed "
                  "by normal operator new");
    VAST_ASSERT(pointer_in_buffer(ptr_) && "short_alloc has outlived arena");
    auto const aligned_n = align_up(n);
    if (static_cast<decltype(aligned_n)>(buf_ + N - ptr_) >= aligned_n) {
      char* r = ptr_;
      ptr_ += aligned_n;
      return r;
    }
    return static_cast<char*>(::operator new(n));
  }

  void deallocate(char* p, size_t n) noexcept {
    VAST_ASSERT(pointer_in_buffer(ptr_) && "short_alloc has outlived arena");
    if (pointer_in_buffer(p)) {
      n = align_up(n);
      if (p + n == ptr_)
        ptr_ = p;
    } else {
      ::operator delete(p);
    }
  }

  static constexpr size_t size() noexcept {
    return N;
  }

  size_t used() const noexcept {
    return static_cast<size_t>(ptr_ - buf_);
  }

  void reset() noexcept {
    ptr_ = buf_;
  }

private:
  static size_t align_up(size_t n) noexcept {
    return (n + (Alignment - 1)) & ~(Alignment - 1);
  }

  bool pointer_in_buffer(char* p) noexcept {
    return buf_ <= p && p <= buf_ + N;
  }

  alignas(Alignment) char buf_[N];
  char* ptr_;
};

template <class T, size_t N, size_t Align = alignof(std::max_align_t)>
class short_alloc {
  template <class U, size_t M, size_t A>
  friend class short_alloc;

public:
  using value_type = T;
  static auto constexpr alignment = Align;
  static auto constexpr size = N;
  using arena_type = arena<size, alignment>;

  static_assert(size % alignment == 0, "N needs to be a multiple of Align");

  short_alloc(const short_alloc&) = default;
  short_alloc& operator=(const short_alloc&) = delete;

  short_alloc(arena_type& a) noexcept : a_(a) {
  }

  template <class U>
  short_alloc(const short_alloc<U, N, alignment>& a) noexcept : a_(a.a_) {
  }

  template <class _Up>
  struct rebind {
    using other = short_alloc<_Up, N, alignment>;
  };

  T* allocate(size_t n) {
    return reinterpret_cast<T*>(a_.template
                                allocate<alignof(T)>(n * sizeof(T)));
  }

  void deallocate(T* p, size_t n) noexcept {
    a_.deallocate(reinterpret_cast<char*>(p), n * sizeof(T));
  }

  template <class T0, size_t N0, size_t A0, class T1, size_t N1, size_t A1>
  friend bool operator==(const short_alloc<T0, N0, A0>&,
                         const short_alloc<T1, N1, A1>&) noexcept;

private:
  arena_type& a_;
};

template <class T0, size_t N0, size_t A0, class T1, size_t N1, size_t A1>
bool operator==(const short_alloc<T0, N0, A0>& x,
                const short_alloc<T1, N1, A1>& y) noexcept {
  return N0 == N1 && A0 == A1 && &x.a_ == &y.a_;
}

template <class T0, size_t N0, size_t A0, class T1, size_t N1, size_t A1>
bool operator!=(const short_alloc<T0, N0, A0>& x,
                const short_alloc<T1, N1, A1>& y) noexcept {
  return !(x == y);
}

} // namespace detail
} // namespace vast

#endif

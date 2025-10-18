//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/allocator.hpp"

#include <mimalloc.h>

namespace {

static_assert(std::align_val_t{__STDCPP_DEFAULT_NEW_ALIGNMENT__}
              == tenzir::memory::mimalloc::allocator::default_alignment);
static_assert(std::align_val_t{__STDCPP_DEFAULT_NEW_ALIGNMENT__}
              == tenzir::memory::system::allocator::default_alignment);

template <bool throws>
[[nodiscard, gnu::hot, gnu::alloc_size(1)]]
auto allocate_cpp(std::size_t size) noexcept(not throws) -> void* {
  static auto& allocator = tenzir::memory::cpp_allocator();
  auto ptr = allocator.allocate(size);
  if constexpr (throws) {
    if (ptr == nullptr) {
      throw std::bad_alloc{};
    }
  }
  return ptr;
}

template <bool throws>
[[nodiscard, gnu::hot, gnu::alloc_size(1), gnu::alloc_align(2)]]
auto allocate_cpp(std::size_t size,
                  std::align_val_t alignment) noexcept(not throws) -> void* {
  static auto& allocator = tenzir::memory::cpp_allocator();
  auto ptr = allocator.allocate(size, alignment);
  if constexpr (throws) {
    if (ptr == nullptr) {
      throw std::bad_alloc{};
    }
  }
  return ptr;
}

auto deallocate_cpp(void* ptr) noexcept -> void {
  auto& allocator = tenzir::memory::cpp_allocator();
  allocator.deallocate(ptr);
}

} // namespace

// (1)
void* operator new(std::size_t size) {
  return allocate_cpp<true>(size);
}
// (2)
void* operator new[](std::size_t size) {
  return allocate_cpp<true>(size);
}
// (3)
void* operator new(std::size_t size, std::align_val_t alignment) {
  return allocate_cpp<true>(size, alignment);
}
// (4)
void* operator new[](std::size_t size, std::align_val_t alignment) {
  return allocate_cpp<true>(size, alignment);
}
// (5)
void* operator new(std::size_t size, const std::nothrow_t&) noexcept {
  return allocate_cpp<false>(size);
}
// (6)
void* operator new[](std::size_t size, const std::nothrow_t&) noexcept {
  return allocate_cpp<false>(size);
}
// (7)
void* operator new(std::size_t size, std::align_val_t alignment,
                   const std::nothrow_t&) noexcept {
  return allocate_cpp<false>(size, alignment);
}
// (8)
void* operator new[](std::size_t size, std::align_val_t alignment,
                     const std::nothrow_t&) noexcept {
  return allocate_cpp<false>(size, alignment);
}

// (1)
void operator delete(void* ptr) noexcept {
  deallocate_cpp(ptr);
}
// (2)
void operator delete[](void* ptr) noexcept {
  deallocate_cpp(ptr);
}
// (3)
void operator delete(void* ptr, std::align_val_t) noexcept {
  deallocate_cpp(ptr);
}
// (4)
void operator delete[](void* ptr, std::align_val_t) noexcept {
  deallocate_cpp(ptr);
}
// (5)
void operator delete(void* ptr, std::size_t) noexcept {
  deallocate_cpp(ptr);
}
// (6)
void operator delete[](void* ptr, std::size_t) noexcept {
  deallocate_cpp(ptr);
}
// (7)
void operator delete(void* ptr, std::size_t, std::align_val_t) noexcept {
  deallocate_cpp(ptr);
}
// (8)
void operator delete[](void* ptr, std::size_t, std::align_val_t) noexcept {
  deallocate_cpp(ptr);
}

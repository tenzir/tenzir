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

constexpr auto default_alignment
  = std::align_val_t{__STDCPP_DEFAULT_NEW_ALIGNMENT__};

auto allocate_cpp(std::size_t size, std::align_val_t alignment) noexcept
  -> void* {
  auto& allocator = tenzir::memory::cpp_allocator();
  auto blk = allocator.allocate(size, alignment);
  return blk.ptr;
}

auto allocate_cpp_throw(std::size_t size, std::align_val_t alignment) -> void* {
  auto* ptr = allocate_cpp(size, alignment);
  if (ptr == nullptr) {
    throw std::bad_alloc{};
  }
  return ptr;
}

auto deallocate_cpp(void* ptr, std::size_t size,
                    std::align_val_t alignment) noexcept {
  auto& allocator = tenzir::memory::cpp_allocator();
  allocator.deallocate(
    tenzir::memory::block{static_cast<std::byte*>(ptr), size}, alignment);
}

} // namespace

// (1)
void* operator new(std::size_t size) {
  return allocate_cpp_throw(size, default_alignment);
}
// (2)
void* operator new[](std::size_t size) {
  return allocate_cpp_throw(size, default_alignment);
}
// (3)
void* operator new(std::size_t size, std::align_val_t alignment) {
  return allocate_cpp_throw(size, alignment);
}
// (4)
void* operator new[](std::size_t size, std::align_val_t alignment) {
  return allocate_cpp_throw(size, alignment);
}
// (5)
void* operator new(std::size_t size, const std::nothrow_t&) noexcept {
  return allocate_cpp(size, default_alignment);
}
// (6)
void* operator new[](std::size_t size, const std::nothrow_t&) noexcept {
  return allocate_cpp(size, default_alignment);
}
// (7)
void* operator new(std::size_t size, std::align_val_t alignment,
                   const std::nothrow_t&) noexcept {
  return allocate_cpp(size, alignment);
}
// (8)
void* operator new[](std::size_t size, std::align_val_t alignment,
                     const std::nothrow_t&) noexcept {
  return allocate_cpp(size, alignment);
}

// (1)
void operator delete(void* ptr) noexcept {
  deallocate_cpp(ptr, 0, default_alignment);
}
// (2)
void operator delete[](void* ptr) noexcept {
  deallocate_cpp(ptr, 0, default_alignment);
}
// (3)
void operator delete(void* ptr, std::align_val_t alignment) noexcept {
  deallocate_cpp(ptr, 0, alignment);
}
// (4)
void operator delete[](void* ptr, std::align_val_t alignment) noexcept {
  deallocate_cpp(ptr, 0, alignment);
}
// (5)
void operator delete(void* ptr, std::size_t size) noexcept {
  deallocate_cpp(ptr, size, default_alignment);
}
// (6)
void operator delete[](void* ptr, std::size_t size) noexcept {
  deallocate_cpp(ptr, size, default_alignment);
}
// (7)
void operator delete(void* ptr, std::size_t size,
                     std::align_val_t alignment) noexcept {
  deallocate_cpp(ptr, size, alignment);
}
// (8)
void operator delete[](void* ptr, std::size_t size,
                       std::align_val_t alignment) noexcept {
  deallocate_cpp(ptr, size, alignment);
}

//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/allocator_config.hpp"

#if TENZIR_SELECT_ALLOCATOR != TENZIR_SELECT_ALLOCATOR_NONE

#  include "tenzir/allocator.hpp"

namespace {

/// We know that mimallocs default alignment is 16:
/// https://github.com/microsoft/mimalloc/blob/v3.1.5/include/mimalloc/types.h#L32-L34
/// This header unfortunately is not installed.
/// We now need to ensure that this is at least as strict as the default
/// alignment of the system malloc, in order to maintain the alignment
/// guarantees on our malloc override. This is not great as it decouples us from
/// the actual value used by mimalloc, however it will only be an issue if we
/// ever compile on a system where the default alignment is 32 bytes.
static_assert(__STDCPP_DEFAULT_NEW_ALIGNMENT__ <= 16,
              "Unexpectedly large default alignment");

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

#endif

//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/allocator.hpp"

#include "tenzir/config.hpp"
#include "tenzir/si_literals.hpp"

#include <caf/actor_system.hpp>

#include <cstdio>

#if TENZIR_LINUX
#  include <malloc.h>
#elif TENZIR_MACOS
#  include <malloc/malloc.h>
#  define malloc_usable_size malloc_size
#endif

#include <mimalloc.h>

namespace tenzir::memory {

auto stats::update_max_bytes(std::int64_t new_usage) noexcept -> void {
  auto old_max = bytes_max.load(std::memory_order_relaxed);
  while (old_max < new_usage
         && ! bytes_max.compare_exchange_weak(old_max, new_usage,
                                              std::memory_order_relaxed)) {
  }
}

auto stats::add_allocation() noexcept -> void {
  auto new_count
    = allocations_current.fetch_add(1, std::memory_order_relaxed) + 1;
  auto old_max = allocations_max.load(std::memory_order_relaxed);
  while (old_max < new_count
         && ! allocations_max.compare_exchange_weak(
           old_max, new_count, std::memory_order_relaxed)) {
  }
}

auto stats::note_allocation(std::int64_t add) noexcept -> void {
  bytes_total.fetch_add(add, std::memory_order_relaxed);
  num_calls.fetch_add(1, std::memory_order_relaxed);
  add_allocation();
  const auto previous_current_usage
    = bytes_current.fetch_add(add, std::memory_order_relaxed);
  const auto new_current_usage = previous_current_usage + add;
  update_max_bytes(new_current_usage);
}

auto stats::note_reallocation(bool new_location, std::int64_t old_size,
                              std::int64_t new_size) noexcept -> void {
  num_calls.fetch_add(1, std::memory_order_relaxed);
  if (new_location) {
    note_deallocation(old_size);
    note_allocation(new_size);
    return;
  } else {
    const auto diff = static_cast<std::int64_t>(new_size)
                      - static_cast<std::int64_t>(old_size);
    const auto previous_current_usage
      = bytes_current.fetch_add(diff, std::memory_order_relaxed);
    if (diff > 0) {
      const auto new_current_usage = previous_current_usage + diff;
      update_max_bytes(new_current_usage);
    }
  }
}

auto stats::note_deallocation(std::int64_t remove) noexcept -> void {
  allocations_current.fetch_sub(1, std::memory_order_relaxed);
  bytes_current.fetch_sub(remove, std::memory_order_relaxed);
}

constexpr auto align_mask(std::align_val_t alignment) noexcept
  -> std::uintptr_t {
  return std::to_underlying(alignment) - 1;
}

constexpr auto is_aligned(void* ptr, std::align_val_t alignment) noexcept
  -> bool {
  return (reinterpret_cast<std::uintptr_t>(ptr) & align_mask(alignment)) == 0;
}

constexpr auto is_aligned(std::size_t size, std::align_val_t alignment) noexcept
  -> bool {
  return (size & align_mask(alignment)) == 0;
}

constexpr auto align(std::size_t size, std::align_val_t alignment) noexcept
  -> std::size_t {
  const auto remainder = size & align_mask(alignment);
  return size + (remainder > 0) * (std::to_underlying(alignment) - remainder);
}

namespace detail {

template <allocator_configuration config>
auto allocator_impl<config>::allocate(std::size_t size) noexcept -> void* {
  auto* const ptr = config.alloc(size);
  if (ptr == nullptr) {
    return {};
  }
  if (stats_) {
    stats_->note_allocation(config.size(ptr));
  }
  return ptr;
}

template <allocator_configuration config>
auto allocator_impl<config>::allocate(std::size_t size,
                                      std::align_val_t alignment) noexcept
  -> void* {
  if (alignment < default_alignment) {
    return allocate(size);
  }
  size = align(size, alignment);
  auto* const ptr = config.alloc_aligned(size, std::to_underlying(alignment));
  if (ptr == nullptr) {
    return {};
  }
  if (stats_) {
    stats_->note_allocation(config.size(ptr));
  }
  return ptr;
}

template <allocator_configuration config>
auto allocator_impl<config>::deallocate(void* ptr) noexcept -> void {
  if (ptr == nullptr) {
    return;
  }
  if (stats_) {
    stats_->note_deallocation(config.size(ptr));
  }
  config.dealloc(ptr);
  return;
}

template <allocator_configuration config>
auto allocator_impl<config>::reallocate(void* old_ptr,
                                        std::size_t new_size) noexcept
  -> void* {
  const auto old_size = config.size(old_ptr);
  if (old_size >= new_size) {
    return old_ptr;
  }
  if (new_size == 0) {
    deallocate(old_ptr);
    return nullptr;
  }
  void* new_ptr = config.realloc(old_ptr, new_size);
  if (new_ptr == nullptr) {
    return nullptr;
  }
  if (stats_) {
    new_size = config.size(new_ptr);
    stats_->note_reallocation(old_ptr != new_ptr, old_size, new_size);
  }
  return new_ptr;
}

template <allocator_configuration config>
auto allocator_impl<config>::reallocate(void* old_ptr, std::size_t new_size,
                                        std::align_val_t alignment) noexcept
  -> void* {
  if (alignment <= config.default_alignment) {
    return reallocate(old_ptr, new_size);
  }
  const auto old_size = config.size(old_ptr);
  if (old_size >= new_size) {
    if (stats_) {
      stats_->note_reallocation(false, old_size, new_size);
    }
    return old_ptr;
  }
  if (new_size == 0) {
    deallocate(old_ptr);
    return nullptr;
  }
  void* new_ptr
    = config.realloc_aligned(old_ptr, new_size, std::to_underlying(alignment));
  if (new_ptr == nullptr) {
    return nullptr;
  }
  if (stats_) {
    new_size = config.size(new_ptr);
    stats_->note_reallocation(old_ptr != new_ptr, old_size, new_size);
  }
  return new_ptr;
}

template class allocator_impl<mimalloc::config>;
template class allocator_impl<system::config>;

} // namespace detail

namespace mimalloc {

namespace {
struct init {
  init() {
    // Configure mimalloc for optimal memory management.
    // These settings balance memory reuse with returning memory to the OS.
    mi_option_set(mi_option_reset_delay, 100);
    mi_option_set(mi_option_reset_decommits, 1);
  }
} init_;
} // namespace

auto trim() noexcept -> void {
  ::mi_collect(false);
}
} // namespace mimalloc

namespace system {

#ifndef __clang__
[[nodiscard, gnu::hot, gnu::malloc(std::free), gnu::alloc_size(1),
  gnu::alloc_align(2)]]
#else
[[nodiscard, gnu::hot, gnu::malloc, gnu::alloc_size(1), gnu::alloc_align(2)]]
#endif
auto malloc_aligned(std::size_t size, std::size_t alignment) noexcept -> void* {
  return std::aligned_alloc(alignment, size);
}

#ifndef __clang__
[[nodiscard, gnu::hot, gnu::malloc(std::free), gnu::alloc_size(2),
  gnu::alloc_align(3)]]
#else
[[nodiscard, gnu::hot, gnu::malloc, gnu::alloc_size(2), gnu::alloc_align(3)]]
#endif
auto realloc_aligned(void* ptr, std::size_t new_size,
                     std::size_t alignment) noexcept -> void* {
  auto* new_ptr_happy = std::realloc(ptr, new_size);
  if (is_aligned(new_ptr_happy, std::align_val_t{alignment})) {
    return new_ptr_happy;
  }
  if (not new_ptr_happy) {
    new_ptr_happy = ptr;
  }
  auto size = malloc_usable_size(new_ptr_happy);
  auto* new_ptr_expensive = malloc_aligned(new_size, alignment);
  std::memcpy(new_ptr_expensive, new_ptr_happy, size);
  free(new_ptr_happy);
  return new_ptr_expensive;
}

auto malloc_size(const void* ptr) noexcept -> std::size_t {
#if TENZIR_LINUX
  return ::malloc_usable_size(const_cast<void*>(ptr));
#endif
#if TENZIR_MACOS
  return ::malloc_size(ptr);
#endif
}

auto trim() noexcept -> void {
#if (defined(__GLIBC__))
  using namespace si_literals;
  constexpr static auto padding = 512_Mi;
  ::malloc_trim(padding);
#endif
}
} // namespace system

auto enable_stats(const char* var_name) noexcept -> bool {
  const auto env = ::getenv(var_name);
  if (not env) {
    return false;
  }
  const auto sv = std::string_view{env};
  return sv == "true" or sv == "1";
}

auto selected_alloc(const char* var_name) noexcept -> enum selected_alloc {
  const auto env = ::getenv(var_name);
  if (not env) {
    return selected_alloc::mimalloc;
  }
  const auto env_str = std::string_view{env};
  if (env_str.empty() or env_str == "mimalloc") {
    return selected_alloc::mimalloc;
  }
  if (env_str == "system") {
    return selected_alloc::system;
  }
  std::fprintf(stderr,
               "FATAL ERROR: unknown TENZIR_ALLOCATOR: '%s'\n"
               "known values are 'mimalloc' and 'system'\n",
               env_str.data());
  std::exit(EXIT_FAILURE);
  std::unreachable();
}

} // namespace tenzir::memory

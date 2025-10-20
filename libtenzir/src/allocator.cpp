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

#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <dlfcn.h>
#include <limits>

#if TENZIR_LINUX
#  include <malloc.h>
#elif TENZIR_MACOS
#  include <malloc/malloc.h>
#  define malloc_usable_size malloc_size
#endif

#include <mimalloc.h>

namespace tenzir::memory {

auto stats::update_max_bytes(std::int64_t new_usage) noexcept -> void {
  auto old_max = bytes_max.load();
  while (old_max < new_usage
         && ! bytes_max.compare_exchange_weak(old_max, new_usage)) {
  }
}

auto stats::add_allocation() noexcept -> void {
  auto new_count
    = allocations_current.fetch_add(1, std::memory_order_relaxed) + 1;
  auto old_max = allocations_max.load();
  while (old_max < new_count
         && ! allocations_max.compare_exchange_weak(old_max, new_count)) {
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
auto allocator_impl<config>::calloc(std::size_t count,
                                    std::size_t size) noexcept -> void* {
  auto* const ptr = config.calloc(count, size);
  if (ptr == nullptr) {
    return {};
  }
  if (stats_) {
    stats_->note_allocation(config.size(ptr));
  }
  return ptr;
}

template <allocator_configuration config>
auto allocator_impl<config>::calloc(std::size_t count, std::size_t size,
                                    std::align_val_t alignment) noexcept
  -> void* {
  if (alignment < default_alignment) {
    return calloc(count, size);
  }
  auto* const ptr
    = config.calloc_aligned(count, size, std::to_underlying(alignment));
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
  if (new_size == 0) {
    deallocate(old_ptr);
    return nullptr;
  }
  const auto old_size = config.size(old_ptr);
  if (old_size >= new_size) {
    return old_ptr;
  }
  void* const new_ptr = config.realloc(old_ptr, new_size);
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
  if (new_size == 0) {
    deallocate(old_ptr);
    return nullptr;
  }
  if (alignment <= config.default_alignment) {
    return reallocate(old_ptr, new_size);
  }
  const auto old_size = config.size(old_ptr);
  if (old_size >= new_size) {
    return old_ptr;
  }
  void* const new_ptr
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

namespace {

[[nodiscard]] constexpr auto
multiply_overflows(std::size_t lhs, std::size_t rhs,
                   std::size_t& result) noexcept -> bool {
#if defined(__has_builtin)
#  if __has_builtin(__builtin_mul_overflow)
  return __builtin_mul_overflow(lhs, rhs, &result);
#  endif
#endif
  if (lhs == 0 || rhs == 0) {
    result = 0;
    return false;
  }
  if (lhs > std::numeric_limits<std::size_t>::max() / rhs) {
    return true;
  }
  result = lhs * rhs;
  return false;
}

template <typename Function>
[[nodiscard]] auto lookup_symbol(const char* name) noexcept -> Function {
  auto resolve = [name]() noexcept -> Function {
    dlerror();
    if (auto* sym = dlsym(RTLD_NEXT, name)) {
      return reinterpret_cast<Function>(sym);
    }
    if (auto* sym = dlsym(RTLD_DEFAULT, name)) {
      return reinterpret_cast<Function>(sym);
    }
    return nullptr;
  };
  static auto fn = resolve();
  return fn;
}

} // namespace

#ifndef __clang__
[[nodiscard, gnu::hot, gnu::malloc(native_free), gnu::alloc_size(1)]]
#else
[[nodiscard, gnu::hot, gnu::malloc, gnu::alloc_size(1)]]
#endif
auto native_malloc(std::size_t size) noexcept -> void* {
  using function_type = void* (*)(std::size_t);
  static auto fn = lookup_symbol<function_type>("malloc");
  if (fn == nullptr) {
    errno = ENOSYS;
    return nullptr;
  }
  return fn(size);
}

#ifndef __clang__
[[nodiscard, gnu::hot, gnu::malloc(native_free), gnu::alloc_size(1, 2)]]
#else
[[nodiscard, gnu::hot, gnu::malloc, gnu::alloc_size(1, 2)]]
#endif
auto native_calloc(std::size_t count, std::size_t size) noexcept -> void* {
  using function_type = void* (*)(std::size_t, std::size_t);
  static auto fn = lookup_symbol<function_type>("calloc");
  if (fn == nullptr) {
    errno = ENOSYS;
    return nullptr;
  }
  return fn(count, size);
}

#ifndef __clang__
[[nodiscard, gnu::hot, gnu::malloc(native_free), gnu::alloc_size(2)]]
#else
[[nodiscard, gnu::hot, gnu::malloc, gnu::alloc_size(2)]]
#endif
auto native_realloc(void* ptr, std::size_t new_size) noexcept -> void* {
  using function_type = void* (*)(void*, std::size_t);
  static auto fn = lookup_symbol<function_type>("realloc");
  if (fn == nullptr) {
    errno = ENOSYS;
    return nullptr;
  }
  return fn(ptr, new_size);
}

#ifndef __clang__
[[nodiscard, gnu::hot, gnu::malloc(native_free), gnu::alloc_size(2, 3)]]
#else
[[nodiscard, gnu::hot, gnu::malloc, gnu::alloc_size(2, 3)]]
#endif
auto native_reallocarray(void* ptr, std::size_t count,
                         std::size_t size) noexcept -> void* {
  std::size_t total = 0;
  if (multiply_overflows(count, size, total)) {
    errno = ENOMEM;
    return nullptr;
  }
  using function_type = void* (*)(void*, std::size_t, std::size_t);
  static auto fn = lookup_symbol<function_type>("reallocarray");
  if (fn != nullptr) {
    return fn(ptr, count, size);
  }
  return native_realloc(ptr, total);
}

[[gnu::hot]]
auto native_free(void* ptr) noexcept -> void {
  using function_type = void (*)(void*);
  static auto fn = lookup_symbol<function_type>("free");
  if (fn == nullptr) {
    return;
  }
  fn(ptr);
}

#ifndef __clang__
[[nodiscard, gnu::hot, gnu::malloc(native_free), gnu::alloc_size(2),
  gnu::alloc_align(1)]]
#else
[[nodiscard, gnu::hot, gnu::malloc, gnu::alloc_size(2), gnu::alloc_align(1)]]
#endif
auto native_memalign(std::size_t alignment, std::size_t size) noexcept
  -> void* {
  using function_type = void* (*)(std::size_t, std::size_t);
  static auto fn = lookup_symbol<function_type>("memalign");
  if (fn != nullptr) {
    return fn(alignment, size);
  }
  using aligned_fn = void* (*)(std::size_t, std::size_t);
  static auto aligned = lookup_symbol<aligned_fn>("aligned_alloc");
  if (aligned != nullptr) {
    return aligned(alignment, size);
  }
  errno = ENOSYS;
  return nullptr;
}

#ifndef __clang__
[[nodiscard, gnu::hot, gnu::malloc(native_free), gnu::alloc_size(2),
  gnu::alloc_align(1)]]
#else
[[nodiscard, gnu::hot, gnu::malloc, gnu::alloc_size(2), gnu::alloc_align(1)]]
#endif
auto native_aligned_alloc(std::size_t alignment, std::size_t size) noexcept
  -> void* {
  using function_type = void* (*)(std::size_t, std::size_t);
  static auto fn = lookup_symbol<function_type>("aligned_alloc");
  if (fn != nullptr) {
    return fn(alignment, size);
  }
  return native_memalign(alignment, size);
}

[[nodiscard, gnu::hot]]
auto native_malloc_usable_size(const void* ptr) noexcept -> std::size_t {
  using function_type = std::size_t (*)(const void*);
#if TENZIR_LINUX
  static auto fn = lookup_symbol<function_type>("malloc_usable_size");
#elif TENZIR_MACOS
  static auto fn = lookup_symbol<function_type>("malloc_size");
#endif
  if (fn != nullptr) {
    return fn(ptr);
  }
  return 0;
}

#ifndef __clang__
[[nodiscard, gnu::hot, gnu::malloc(native_free), gnu::alloc_size(1),
  gnu::alloc_align(2)]]
#else
[[nodiscard, gnu::hot, gnu::malloc, gnu::alloc_size(1), gnu::alloc_align(2)]]
#endif
auto malloc_aligned(std::size_t size, std::size_t alignment) noexcept -> void* {
  return native_aligned_alloc(alignment, size);
}

#ifndef __clang__
[[nodiscard, gnu::hot, gnu::malloc(native_free), gnu::alloc_size(2),
  gnu::alloc_align(3)]]
#else
[[nodiscard, gnu::hot, gnu::malloc, gnu::alloc_size(2), gnu::alloc_align(3)]]
#endif
auto realloc_aligned(void* ptr, std::size_t new_size,
                     std::size_t alignment) noexcept -> void* {
  auto* new_ptr_happy = native_realloc(ptr, new_size);
  if (is_aligned(new_ptr_happy, std::align_val_t{alignment})) {
    return new_ptr_happy;
  }
  if (not new_ptr_happy) {
    new_ptr_happy = ptr;
  }
  auto size = malloc_usable_size(new_ptr_happy);
  auto* new_ptr_expensive = malloc_aligned(new_size, alignment);
  std::memcpy(new_ptr_expensive, new_ptr_happy, size);
  native_free(new_ptr_happy);
  return new_ptr_expensive;
}

#ifndef __clang__
[[nodiscard, gnu::hot, gnu::malloc(native_free), gnu::alloc_size(1, 2),
  gnu::alloc_align(3)]]
#else
[[nodiscard, gnu::hot, gnu::malloc, gnu::alloc_size(1, 2), gnu::alloc_align(3)]]
#endif
auto calloc_aligned(std::size_t count, std::size_t size,
                    std::size_t alignment) noexcept -> void* {
  std::size_t total = 0;
  if (multiply_overflows(count, size, total)) {
    errno = ENOMEM;
    return nullptr;
  }
  auto* ptr = malloc_aligned(total, alignment);
  if (ptr != nullptr) {
    std::memset(ptr, 0, total);
  }
  return ptr;
}

auto malloc_size(const void* ptr) noexcept -> std::size_t {
  return native_malloc_usable_size(ptr);
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

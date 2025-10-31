//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/allocator.hpp"

#include "tenzir/concept/parseable/tenzir/time.hpp"
#include "tenzir/config.hpp"
#include "tenzir/logger.hpp"
#include "tenzir/si_literals.hpp"

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

} // namespace mimalloc

namespace system {

namespace {

constexpr auto align_mask(std::align_val_t alignment) noexcept
  -> std::uintptr_t {
  return std::to_underlying(alignment) - 1;
}

constexpr auto is_aligned(void* ptr, std::align_val_t alignment) noexcept
  -> bool {
  return (reinterpret_cast<std::uintptr_t>(ptr) & align_mask(alignment)) == 0;
}

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

auto write_error(const char* txt) noexcept {
  write(STDERR_FILENO, txt, strlen(txt));
}

template <typename Function>
[[nodiscard]] auto lookup_symbol(const char* name) noexcept -> Function {
  dlerror();
  if (auto* sym = dlsym(RTLD_NEXT, name)) {
    return reinterpret_cast<Function>(sym);
  }
  write_error("failed to lookup symbol ");
  write_error(name);
  std::exit(EXIT_FAILURE);
}

} // namespace

#ifndef __clang__
[[nodiscard, gnu::hot, gnu::malloc(native_free), gnu::alloc_size(1)]]
#else
[[nodiscard, gnu::hot, gnu::malloc, gnu::alloc_size(1)]]
#endif
auto native_malloc(std::size_t size) noexcept -> void* {
#if TENZIR_ENABLE_STATIC_EXECUTABLE
  void* __real_malloc(size_t);
  return __real_malloc(size);
#else
  using function_type = void* (*)(std::size_t);
  const static auto fn = lookup_symbol<function_type>("malloc");
  return fn(size);
#endif
}

#ifndef __clang__
[[nodiscard, gnu::hot, gnu::malloc(native_free), gnu::alloc_size(1, 2)]]
#else
[[nodiscard, gnu::hot, gnu::malloc, gnu::alloc_size(1, 2)]]
#endif
auto native_calloc(std::size_t count, std::size_t size) noexcept -> void* {
#if TENZIR_ENABLE_STATIC_EXECUTABLE
  void* __real_calloc(size_t, size_t);
  return __real_calloc(count, size);
#else
  using function_type = void* (*)(std::size_t, std::size_t);
  const static auto fn = lookup_symbol<function_type>("calloc");
  if (fn == nullptr) {
    errno = ENOSYS;
    return nullptr;
  }
  return fn(count, size);
#endif
}

#ifndef __clang__
[[nodiscard, gnu::hot, gnu::malloc(native_free), gnu::alloc_size(2)]]
#else
[[nodiscard, gnu::hot, gnu::malloc, gnu::alloc_size(2)]]
#endif
auto native_realloc(void* ptr, std::size_t new_size) noexcept -> void* {
#if TENZIR_ENABLE_STATIC_EXECUTABLE
  void* __real_realloc(void*, size_t);
  return __real_realloc(ptr, new_size);
#else
  using function_type = void* (*)(void*, std::size_t);
  const static auto fn = lookup_symbol<function_type>("realloc");
  return fn(ptr, new_size);
#endif
}

[[gnu::hot]]
auto native_free(void* ptr) noexcept -> void {
#if TENZIR_ENABLE_STATIC_EXECUTABLE
  void __real_free(void*);
  return __real_free(ptr);
#else
  using function_type = void (*)(void*);
  static auto fn = lookup_symbol<function_type>("free");
  fn(ptr);
#endif
}

#ifndef __clang__
[[nodiscard, gnu::hot, gnu::malloc(native_free), gnu::alloc_size(2),
  gnu::alloc_align(1)]]
#else
[[nodiscard, gnu::hot, gnu::malloc, gnu::alloc_size(2), gnu::alloc_align(1)]]
#endif
auto native_aligned_alloc(std::size_t alignment, std::size_t size) noexcept
  -> void* {
#if TENZIR_ENABLE_STATIC_EXECUTABLE
  void* __real_aligned_alloc(size_t, size_t);
  return __real_aligned_alloc(alignment, size);
#else
  using function_type = void* (*)(std::size_t, std::size_t);
  const static auto fn = lookup_symbol<function_type>("aligned_alloc");
  return fn(alignment, size);
#endif
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
  // `realloc` does not give alignment guarantees. Because of this, we need to
  // do more work. First we try to reallocate normally
  auto* new_ptr_happy = native_realloc(ptr, new_size);
  // The the allocation satisfies our requirement, we return it
  if (new_ptr_happy
      and is_aligned(new_ptr_happy, std::align_val_t{alignment})) {
    return new_ptr_happy;
  }
  // If reallocation did not happen at all, we need to reconsider the old
  // allocation. Doing this renaming here joins the code paths where realloc
  // failed entirely.
  if (not new_ptr_happy) {
    new_ptr_happy = ptr;
  }
  // Capture the currently allocated size, which we need for memcpy
  const auto size = native_malloc_usable_size(new_ptr_happy);
  // Make an aligned alloaction
  auto* new_ptr_expensive = malloc_aligned(new_size, alignment);
  // Copy over at most as much as the previous allocation contained.
  std::memcpy(new_ptr_expensive, new_ptr_happy, size);
  // Free the old allocation.
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
  if (alignment < alignof(std::max_align_t)) {
    return native_calloc(count, size);
  }
  std::size_t total = 0;
  if (multiply_overflows(count, size, total)) {
    errno = ENOMEM;
    return nullptr;
  }
  /// Try calloc first. There is a good chance it will satisfy our requirement.
  auto* ptr = calloc(count, size);
  if (ptr and is_aligned(ptr, std::align_val_t{alignment})) {
    return ptr;
  }
  /// Otherwise we have to bit the very bitter bullet: Do an aligned
  /// allocation and memset it.
  ptr = malloc_aligned(total, alignment);
  if (ptr != nullptr) {
    std::memset(ptr, 0, total);
  }
  return ptr;
}

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

auto trim() noexcept -> void {
#if (defined(__GLIBC__))
  using namespace si_literals;
  constexpr static auto padding = 512_Mi;
  ::malloc_trim(padding);
#endif
}

} // namespace system

namespace {
auto write_error(const char* txt) noexcept -> void {
  write(STDERR_FILENO, txt, strlen(txt));
}

auto enable_stats_impl(const char* env_name) noexcept -> bool {
  const auto env = ::getenv(env_name);
  if (not env) {
    return false;
  }
  const auto sv = std::string_view{env};
  return sv == "true" or sv == "1";
}

auto selected_backend_impl(const char* env_name) noexcept
  -> std::optional<enum backend> {
  const auto env_value = ::getenv(env_name);
  if (not env_value) {
    return std::nullopt;
  }
  const auto env_str = std::string_view{env_value};
  if (env_str == "mimalloc") {
    return backend::mimalloc;
  }
  if (env_str == "system") {
    return backend::system;
  }
  write_error("FATAL ERROR: unknown '");
  write_error(env_name);
  write_error("' = '");
  write_error(env_value);
  write_error("'\nknown values are 'mimalloc' and 'system'\n");
  std::exit(EXIT_FAILURE);
}
} // namespace

auto enable_stats(const char* env_name) noexcept -> bool {
  if (enable_stats_impl(env_name)) {
    return true;
  }
  return enable_stats_impl("TENZIR_ALLOC_STATS");
}

auto selected_backend(const char* var_name) noexcept -> enum backend {
  if (auto v = selected_backend_impl(var_name)) {
    return *v;
  }
  if (auto v = selected_backend_impl("TENZIR_ALLOC")) {
    return *v;
  }
  return backend::mimalloc;
}

auto trim_interval() noexcept -> tenzir::duration {
  using namespace std::chrono_literals;
  constexpr static auto var_name = "TENZIR_ALLOC_TRIM_INTERVAL";
  constexpr static auto default_interval = duration{10min};
  const auto env = ::getenv(var_name);
  if (not env) {
    return default_interval;
  }
  auto res = duration{default_interval};
  const auto sv = std::string_view{env};
  auto begin = sv.begin();
  auto end = sv.end();
  if (not parsers::simple_duration.parse(begin, end, res)) {
    TENZIR_WARN("failed to parsed environment variable `{}={}`; Using {}",
                var_name, sv, res);
  }
  return res;
}

} // namespace tenzir::memory

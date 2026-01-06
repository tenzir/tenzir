//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include "tenzir/allocator_config.hpp"

#include <tsl/robin_map.h>

#include <atomic>
#include <concepts>
#include <cstddef>
#if TENZIR_ALLOCATOR_HAS_JEMALLOC
#  include <jemalloc/jemalloc.h>
#endif
#if TENZIR_ALLOCATOR_HAS_MIMALLOC
#  include <mimalloc.h>
#endif
#include <new>
#include <string_view>

namespace tenzir::memory {

struct stats {
  alignas(std::hardware_destructive_interference_size)
    std::atomic<std::int64_t> bytes_current;
  alignas(std::hardware_destructive_interference_size)
    std::atomic<std::int64_t> bytes_cumulative;
  alignas(std::hardware_destructive_interference_size)
    std::atomic<std::int64_t> bytes_peak;
  alignas(std::hardware_destructive_interference_size)
    std::atomic<std::int64_t> num_calls;
  alignas(std::hardware_destructive_interference_size)
    std::atomic<std::int64_t> allocations_current;
  alignas(std::hardware_destructive_interference_size)
    std::atomic<std::int64_t> allocations_cumulative;
  alignas(std::hardware_destructive_interference_size)
    std::atomic<std::int64_t> allocations_peak;

  auto note_allocation(std::int64_t add) noexcept -> void;
  auto note_reallocation(bool new_location, std::int64_t old_size,
                         std::int64_t new_size) noexcept -> void;
  auto note_deallocation(std::int64_t remove) noexcept -> void;
  auto update_max_bytes(std::int64_t new_usage) noexcept -> void;
};

enum class backend {
  system,
  jemalloc,
  mimalloc,
};

template <typename T>
concept allocator = requires(T t, std::size_t size, std::align_val_t alignment,
                             void* ptr, const void* cptr) {
  { t.allocate(size) } noexcept -> std::same_as<void*>;
  { t.allocate(size, alignment) } noexcept -> std::same_as<void*>;
  { t.calloc(size, size) } noexcept -> std::same_as<void*>;
  { t.calloc(size, size, alignment) } noexcept -> std::same_as<void*>;
  { t.reallocate(ptr, size) } noexcept -> std::same_as<void*>;
  { t.reallocate(ptr, size, alignment) } noexcept -> std::same_as<void*>;
  { t.deallocate(ptr) } noexcept -> std::same_as<void>;
  { t.trim() } noexcept;
  { t.stats() } noexcept -> std::same_as<const stats&>;
  { t.backend() } noexcept -> std::same_as<enum backend>;
  { t.backend_name() } noexcept -> std::same_as<std::string_view>;
  { t.size(cptr) } noexcept -> std::same_as<std::size_t>;
};

// Polymorphic base class for the runtime switchable allocator.
struct polymorphic_allocator {
  [[nodiscard, gnu::hot, gnu::alloc_size(2)]]
  virtual auto allocate(std::size_t) noexcept -> void* = 0;
  [[nodiscard, gnu::hot, gnu::alloc_size(2), gnu::alloc_align(3)]]
  virtual auto allocate(std::size_t, std::align_val_t) noexcept -> void* = 0;
  [[nodiscard, gnu::hot, gnu::alloc_size(2, 3)]]
  virtual auto calloc(std::size_t, std::size_t) noexcept -> void* = 0;
  [[nodiscard, gnu::hot, gnu::alloc_size(2, 3), gnu::alloc_align(4)]]
  virtual auto calloc(std::size_t, std::size_t, std::align_val_t) noexcept
    -> void* = 0;
  [[nodiscard, gnu::hot, gnu::alloc_size(3)]]
  virtual auto reallocate(void*, std::size_t) noexcept -> void* = 0;
  [[nodiscard, gnu::hot, gnu::alloc_size(3), gnu::alloc_align(4)]]
  virtual auto reallocate(void*, std::size_t, std::align_val_t) noexcept
    -> void* = 0;
  virtual auto deallocate(void*) noexcept -> void = 0;
  virtual auto size(const void*) const noexcept -> std::size_t = 0;
  virtual auto trim() noexcept -> void = 0;
  virtual auto stats() noexcept -> const struct stats& = 0;
  virtual auto backend() const noexcept -> enum backend = 0;
  virtual auto backend_name() const noexcept -> std::string_view = 0;
};

namespace detail {

/// We refer to this object when calling allocator.stats() on an allocator that
/// does not collect stats.
inline constexpr auto zero_stats = stats{};

alignas(__STDCPP_DEFAULT_NEW_ALIGNMENT__) inline std::byte
  zero_size_area[__STDCPP_DEFAULT_NEW_ALIGNMENT__];

/// Concept for allocator traits types.
template <typename T>
concept allocator_trait
  = requires(std::size_t size, std::size_t alignment, std::size_t count,
             void* ptr, const void* cptr) {
      { T::backend_value } -> std::same_as<const backend&>;
      { T::name } -> std::convertible_to<std::string_view>;
      { T::malloc(size) } -> std::same_as<void*>;
      { T::malloc_aligned(size, alignment) } -> std::same_as<void*>;
      { T::calloc(count, size) } -> std::same_as<void*>;
      { T::calloc_aligned(count, size, alignment) } -> std::same_as<void*>;
      { T::realloc(ptr, size) } -> std::same_as<void*>;
      { T::realloc_aligned(ptr, size, alignment) } -> std::same_as<void*>;
      { T::free(ptr) } -> std::same_as<void>;
      { T::usable_size(cptr) } -> std::same_as<std::size_t>;
      { T::trim() } -> std::same_as<void>;
    };

/// Template allocator implementation that delegates to a traits type.
template <allocator_trait Traits>
class basic_allocator final : public polymorphic_allocator {
public:
  constexpr explicit basic_allocator(struct stats* stats) : stats_{stats} {
  }

  [[nodiscard, gnu::hot, gnu::always_inline, gnu::malloc, gnu::alloc_size(2)]]
  auto allocate(std::size_t size) noexcept -> void* final override {
    if (size == 0) {
      return &zero_size_area;
    }
    auto* const ptr = Traits::malloc(size);
    if (ptr == nullptr) {
      return nullptr;
    }
    if (stats_) {
      stats_->note_allocation(Traits::usable_size(ptr));
    }
    return ptr;
  }

  [[nodiscard, gnu::hot, gnu::always_inline, gnu::malloc, gnu::alloc_size(2),
    gnu::alloc_align(3)]]
  auto allocate(std::size_t size, std::align_val_t alignment) noexcept
    -> void* final override {
    if (size == 0) {
      return &zero_size_area;
    }
    auto* const ptr
      = Traits::malloc_aligned(size, std::to_underlying(alignment));
    if (ptr == nullptr) {
      return nullptr;
    }
    if (stats_) {
      stats_->note_allocation(Traits::usable_size(ptr));
    }
    return ptr;
  }

  [[nodiscard, gnu::hot, gnu::always_inline, gnu::malloc,
    gnu::alloc_size(2, 3)]]
  auto calloc(std::size_t count, std::size_t size) noexcept
    -> void* final override {
    if (size * count == 0) {
      return &zero_size_area;
    }
    auto* const ptr = Traits::calloc(count, size);
    if (ptr == nullptr) {
      return nullptr;
    }
    if (stats_) {
      stats_->note_allocation(Traits::usable_size(ptr));
    }
    return ptr;
  }

  [[nodiscard, gnu::hot, gnu::always_inline, gnu::malloc, gnu::alloc_size(2, 3),
    gnu::alloc_align(4)]]
  auto calloc(std::size_t count, std::size_t size,
              std::align_val_t alignment) noexcept -> void* final override {
    if (size * count == 0) {
      return &zero_size_area;
    }
    auto* const ptr
      = Traits::calloc_aligned(count, size, std::to_underlying(alignment));
    if (ptr == nullptr) {
      return nullptr;
    }
    if (stats_) {
      stats_->note_allocation(Traits::usable_size(ptr));
    }
    return ptr;
  }

  [[nodiscard, gnu::hot, gnu::always_inline, gnu::malloc, gnu::alloc_size(3)]]
  auto reallocate(void* old_ptr, std::size_t new_size) noexcept
    -> void* final override {
    if (new_size == 0) {
      deallocate(old_ptr);
      return &zero_size_area;
    }
    if (old_ptr == &zero_size_area) {
      return allocate(new_size);
    }
    const auto old_size = Traits::usable_size(old_ptr);
    void* const new_ptr = Traits::realloc(old_ptr, new_size);
    if (new_ptr == nullptr) {
      return nullptr;
    }
    if (stats_) {
      const auto actual_new_size = Traits::usable_size(new_ptr);
      stats_->note_reallocation(old_ptr != new_ptr, old_size, actual_new_size);
    }
    return new_ptr;
  }

  [[nodiscard, gnu::hot, gnu::always_inline, gnu::malloc, gnu::alloc_size(3),
    gnu::alloc_align(4)]]
  auto reallocate(void* old_ptr, std::size_t new_size,
                  std::align_val_t alignment) noexcept -> void* final override {
    if (new_size == 0) {
      deallocate(old_ptr);
      return &zero_size_area;
    }
    if (old_ptr == &zero_size_area) {
      return allocate(new_size, alignment);
    }
    const auto old_size = Traits::usable_size(old_ptr);
    void* const new_ptr = Traits::realloc_aligned(
      old_ptr, new_size, std::to_underlying(alignment));
    if (new_ptr == nullptr) {
      return nullptr;
    }
    if (stats_) {
      const auto actual_new_size = Traits::usable_size(new_ptr);
      stats_->note_reallocation(old_ptr != new_ptr, old_size, actual_new_size);
    }
    return new_ptr;
  }

  [[gnu::hot, gnu::always_inline]]
  auto deallocate(void* ptr) noexcept -> void final override {
    if (ptr == nullptr) {
      return;
    }
    if (ptr == &zero_size_area) {
      return;
    }
    if (stats_) {
      stats_->note_deallocation(Traits::usable_size(ptr));
    }
    Traits::free(ptr);
  }

  [[gnu::hot, gnu::always_inline]]
  auto size(const void* ptr) const noexcept -> std::size_t final override {
    return Traits::usable_size(ptr);
  }

  auto trim() noexcept -> void final override {
    Traits::trim();
  }

  [[nodiscard]]
  auto stats() noexcept -> const struct stats& final override {
    return stats_ ? *stats_ : zero_stats;
  }

  [[nodiscard]]
  auto backend() const noexcept -> enum backend final override {
    return Traits::backend_value;
  }

  [[nodiscard]]
  auto backend_name() const noexcept -> std::string_view final override {
    return Traits::name;
  }

private:
  struct stats* const stats_{nullptr};
};

} // namespace detail

#if TENZIR_ALLOCATOR_HAS_JEMALLOC

namespace jemalloc {

#  if TENZIR_GCC
[[nodiscard, gnu::hot, gnu::malloc(je_tenzir_free), gnu::alloc_size(1),
  gnu::alloc_align(2)]]
#  elif TENZIR_CLANG
[[nodiscard, gnu::hot, gnu::malloc, gnu::alloc_size(1), gnu::alloc_align(2)]]
#  endif
/// Simple helper that switches the arguments for `alloc_aligned` for consistency.
auto je_tenzir_malloc_aligned(std::size_t size, std::size_t alignment) noexcept
  -> void*;

#  if TENZIR_GCC
[[nodiscard, gnu::hot, gnu::malloc(je_tenzir_free), gnu::alloc_size(2),
  gnu::alloc_align(3)]]
#  elif TENZIR_CLANG
[[nodiscard, gnu::hot, gnu::malloc, gnu::alloc_size(2), gnu::alloc_align(3)]]
#  endif
/// We fake our own `realloc_aligned`, as that does not exist in C or POSIX.
auto je_tenzir_realloc_aligned(void* ptr, std::size_t new_size,
                               std::size_t alignment) noexcept -> void*;

#  if TENZIR_GCC
[[nodiscard, gnu::hot, gnu::malloc(je_tenzir_free), gnu::alloc_size(1, 2),
  gnu::alloc_align(3)]]
#  elif TENZIR_CLANG
[[nodiscard, gnu::hot, gnu::malloc, gnu::alloc_size(1, 2), gnu::alloc_align(3)]]
#  endif
/// We fake our own `calloc_aligned`, as that does not exist in C or POSIX.
auto je_tenzir_calloc_aligned(std::size_t count, std::size_t size,
                              std::size_t alignment) noexcept -> void*;

/// Wrapper for usable_size that accepts const void*.
inline auto je_tenzir_malloc_usable_size_const(const void* ptr) noexcept
  -> std::size_t {
  return ::je_tenzir_malloc_usable_size(const_cast<void*>(ptr));
}

/// No-op trim for jemalloc (doesn't have a trim / collect).
inline auto trim_noop() noexcept -> void {
}

struct traits {
  static constexpr auto backend_value = backend::jemalloc;
  static constexpr std::string_view name = "jemalloc";

  static constexpr auto malloc = &::je_tenzir_malloc;
  static constexpr auto malloc_aligned = &je_tenzir_malloc_aligned;
  static constexpr auto calloc = &::je_tenzir_calloc;
  static constexpr auto calloc_aligned = &je_tenzir_calloc_aligned;
  static constexpr auto realloc = &::je_tenzir_realloc;
  static constexpr auto realloc_aligned = &je_tenzir_realloc_aligned;
  static constexpr auto free = &::je_tenzir_free;
  static constexpr auto usable_size = &je_tenzir_malloc_usable_size_const;
  static constexpr auto trim = &trim_noop;
};

static_assert(memory::detail::allocator_trait<traits>);

using allocator = memory::detail::basic_allocator<traits>;

} // namespace jemalloc
#endif

#if TENZIR_ALLOCATOR_HAS_MIMALLOC
namespace mimalloc {

/// Trim wrapper that calls mi_collect.
inline auto trim_collect() noexcept -> void {
  ::mi_collect(true);
}

struct traits {
  static constexpr auto backend_value = backend::mimalloc;
  static constexpr std::string_view name = "mimalloc";

  static constexpr auto malloc = &::mi_malloc;
  static constexpr auto malloc_aligned = &::mi_malloc_aligned;
  static constexpr auto calloc = &::mi_calloc;
  static constexpr auto calloc_aligned = &::mi_calloc_aligned;
  static constexpr auto realloc = &::mi_realloc;
  static constexpr auto realloc_aligned = &::mi_realloc_aligned;
  static constexpr auto free = &::mi_free;
  static constexpr auto usable_size = &::mi_malloc_usable_size;
  static constexpr auto trim = &trim_collect;
};

static_assert(memory::detail::allocator_trait<traits>);

using allocator = memory::detail::basic_allocator<traits>;

} // namespace mimalloc
#endif

#if TENZIR_ALLOCATOR_MAY_USE_SYSTEM
namespace system {

[[gnu::hot]] auto trim() noexcept -> void;

}
#endif

#if TENZIR_ALLOCATOR_HAS_SYSTEM
namespace system {

[[gnu::hot]]
/// Function that will call the systems `free`, regardless of our overrides.
auto native_free(void* ptr) noexcept -> void;

#  ifndef __clang__
[[nodiscard, gnu::hot, gnu::malloc(native_free), gnu::alloc_size(1)]]
#  else
[[nodiscard, gnu::hot, gnu::malloc, gnu::alloc_size(1)]]
#  endif
/// Function that will call the systems `malloc`, regardless of our overrides.
auto native_malloc(std::size_t size) noexcept -> void*;

#  ifndef __clang__
[[nodiscard, gnu::hot, gnu::malloc(native_free), gnu::alloc_size(1, 2)]]
#  else
[[nodiscard, gnu::hot, gnu::malloc, gnu::alloc_size(1, 2)]]
#  endif
/// Function that will call the systems `calloc`, regardless of our overrides.
auto native_calloc(std::size_t count, std::size_t size) noexcept -> void*;

#  ifndef __clang__
[[nodiscard, gnu::hot, gnu::malloc(native_free), gnu::alloc_size(2)]]
#  else
[[nodiscard, gnu::hot, gnu::malloc, gnu::alloc_size(2)]]
#  endif
/// Function that will call the systems `realloc`, regardless of our overrides.
auto native_realloc(void* ptr, std::size_t new_size) noexcept -> void*;

[[nodiscard, gnu::hot]]
/// Function that will call the systems `malloc_usable_size`, regardless of our
/// overrides.
auto native_malloc_usable_size(const void* ptr) noexcept -> std::size_t;

#  ifndef __clang__
[[nodiscard, gnu::hot, gnu::malloc(native_free), gnu::alloc_size(1),
  gnu::alloc_align(2)]]
#  else
[[nodiscard, gnu::hot, gnu::malloc, gnu::alloc_size(1), gnu::alloc_align(2)]]
#  endif
/// Simple helper that switches the arguments for `alloc_aligned` for consistency.
auto malloc_aligned(std::size_t size, std::size_t alignment) noexcept -> void*;

#  ifndef __clang__
[[nodiscard, gnu::hot, gnu::malloc(native_free), gnu::alloc_size(2),
  gnu::alloc_align(3)]]
#  else
[[nodiscard, gnu::hot, gnu::malloc, gnu::alloc_size(2), gnu::alloc_align(3)]]
#  endif
/// We fake our own `realloc_aligned`, as that does not exist in C or POSIX.
auto realloc_aligned(void* ptr, std::size_t new_size,
                     std::size_t alignment) noexcept -> void*;

#  ifndef __clang__
[[nodiscard, gnu::hot, gnu::malloc(native_free), gnu::alloc_size(1, 2),
  gnu::alloc_align(3)]]
#  else
[[nodiscard, gnu::hot, gnu::malloc, gnu::alloc_size(1, 2), gnu::alloc_align(3)]]
#  endif
/// We fake our own `calloc_aligned`, as that does not exist in C or POSIX.
auto calloc_aligned(std::size_t count, std::size_t size,
                    std::size_t alignment) noexcept -> void*;

struct traits {
  static constexpr auto backend_value = backend::system;
  static constexpr std::string_view name = "system";

  static constexpr auto malloc = &native_malloc;
  static constexpr auto malloc_aligned = &system::malloc_aligned;
  static constexpr auto calloc = &native_calloc;
  static constexpr auto calloc_aligned = &system::calloc_aligned;
  static constexpr auto realloc = &native_realloc;
  static constexpr auto realloc_aligned = &system::realloc_aligned;
  static constexpr auto free = &native_free;
  static constexpr auto usable_size = &native_malloc_usable_size;
  static constexpr auto trim = &system::trim;
};

static_assert(memory::detail::allocator_trait<traits>);

using allocator = memory::detail::basic_allocator<traits>;

} // namespace system
#endif

[[gnu::const]]
/// Checks if stats collection is enabled for the specific component or a in
/// general.
auto enable_stats(const char* env) noexcept -> bool;

[[gnu::const]]
/// Gets the trim interval from the environment.
auto trim_interval() noexcept -> tenzir::duration;

#if TENZIR_SELECT_ALLOCATOR == TENZIR_SELECT_ALLOCATOR_RUNTIME

[[gnu::const]]
/// Checks if an allocator was requested for the specific component or a
/// non-specific one was set. Returns the default otherwise.
auto selected_backend(const char* env) noexcept -> backend;

#  define TENZIR_MAKE_ALLOCATOR(NAME, ENV_SUFFIX)                              \
    [[nodiscard, gnu::hot, gnu::const]] inline auto NAME() noexcept            \
      -> polymorphic_allocator& {                                              \
      constinit static auto stats_ = stats{};                                  \
      static auto jemalloc_ = jemalloc::allocator{                             \
        enable_stats("TENZIR_ALLOC_STATS_" ENV_SUFFIX) ? &stats_ : nullptr};   \
      static auto mimalloc_ = mimalloc::allocator{                             \
        enable_stats("TENZIR_ALLOC_STATS_" ENV_SUFFIX) ? &stats_ : nullptr};   \
      static auto system_ = system::allocator{                                 \
        enable_stats("TENZIR_ALLOC_STATS_" ENV_SUFFIX) ? &stats_ : nullptr};   \
      static auto& instance = [] {                                             \
        switch (selected_backend("TENZIR_ALLOC_" ENV_SUFFIX)) {                \
          case backend::jemalloc:                                              \
            return static_cast<polymorphic_allocator&>(jemalloc_);             \
          case backend::mimalloc:                                              \
            return static_cast<polymorphic_allocator&>(mimalloc_);             \
          case backend::system_:                                               \
            return static_cast<polymorphic_allocator&>(system_);               \
        }                                                                      \
        TENZIR_UNREACHABLE();                                                  \
      }();                                                                     \
      return instance;

#elif TENZIR_SELECT_ALLOCATOR == TENZIR_SELECT_ALLOCATOR_JEMALLOC

#  define TENZIR_MAKE_ALLOCATOR(NAME, ENV_SUFFIX)                              \
    [[nodiscard, gnu::hot, gnu::const]] inline auto NAME() noexcept            \
      -> jemalloc::allocator& {                                                \
      constinit static auto stats_ = stats{};                                  \
      static auto instance = jemalloc::allocator{                              \
        enable_stats("TENZIR_ALLOC_STATS_" ENV_SUFFIX) ? &stats_ : nullptr};   \
      return instance;                                                         \
    }

#elif TENZIR_SELECT_ALLOCATOR == TENZIR_SELECT_ALLOCATOR_MIMALLOC

#  define TENZIR_MAKE_ALLOCATOR(NAME, ENV_SUFFIX)                              \
    [[nodiscard, gnu::hot, gnu::const]] inline auto NAME() noexcept            \
      -> mimalloc::allocator& {                                                \
      constinit static auto stats_ = stats{};                                  \
      static auto instance = mimalloc::allocator{                              \
        enable_stats("TENZIR_ALLOC_STATS_" ENV_SUFFIX) ? &stats_ : nullptr};   \
      return instance;                                                         \
    }

#elif TENZIR_SELECT_ALLOCATOR == TENZIR_SELECT_ALLOCATOR_SYSTEM

#  define TENZIR_MAKE_ALLOCATOR(NAME, ENV_SUFFIX)                              \
    [[nodiscard, gnu::hot, gnu::const]] inline auto NAME() noexcept            \
      -> system::allocator& {                                                  \
      constinit static auto stats_ = stats{};                                  \
      static auto instance = system::allocator{                                \
        enable_stats("TENZIR_ALLOC_STATS_" ENV_SUFFIX) ? &stats_ : nullptr};   \
      return instance;                                                         \
    }

#else

/// This dummy allocator only exists to make the memory stats compile without
/// issue.
struct dummy_allocator {
  static auto stats() noexcept -> const stats& {
    return detail::zero_stats;
  }
};

#  define TENZIR_MAKE_ALLOCATOR(NAME, ENV_SUFFIX)                              \
    [[nodiscard]] inline auto NAME() noexcept -> dummy_allocator& {            \
      constinit static auto instance = dummy_allocator{};                      \
      return instance;                                                         \
    }

#endif

/// The allocator used by the arrow memory pool, so for all arrow *buffers*.
TENZIR_MAKE_ALLOCATOR(arrow_allocator, "ARROW")
/// The allocator used by `operator new` and `operator delete`.
TENZIR_MAKE_ALLOCATOR(cpp_allocator, "CPP")
/// The allocator used by `malloc` and other C/POSIX allocation functions.
TENZIR_MAKE_ALLOCATOR(c_allocator, "C")

#undef TENZIR_MAKE_ALLOCATOR

} // namespace tenzir::memory

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
#include <mimalloc.h>
#include <new>
#include <string_view>

namespace tenzir::memory {

struct stats {
  alignas(std::hardware_destructive_interference_size)
    std::atomic<std::int64_t> bytes_current;
  alignas(std::hardware_destructive_interference_size)
    std::atomic<std::int64_t> bytes_total;
  alignas(std::hardware_destructive_interference_size)
    std::atomic<std::int64_t> bytes_max;
  alignas(std::hardware_destructive_interference_size)
    std::atomic<std::int64_t> num_calls;
  alignas(std::hardware_destructive_interference_size)
    std::atomic<std::int64_t> allocations_current;
  alignas(std::hardware_destructive_interference_size)
    std::atomic<std::int64_t> allocations_total;
  alignas(std::hardware_destructive_interference_size)
    std::atomic<std::int64_t> allocations_max;

  auto note_allocation(std::int64_t add) noexcept -> void;
  auto note_reallocation(bool new_location, std::int64_t old_size,
                         std::int64_t new_size) noexcept -> void;
  auto note_deallocation(std::int64_t remove) noexcept -> void;
  auto update_max_bytes(std::int64_t new_usage) noexcept -> void;
  auto add_allocation() noexcept -> void;
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
  { t.backend() } noexcept -> std::same_as<std::string_view>;
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
  virtual auto backend() const noexcept -> std::string_view = 0;
};

namespace detail {

/// We refer to this object when calling allocator.stats() on an allocator that
/// does not collect stats.
inline constexpr auto zero_stats = stats{};

alignas(__STDCPP_DEFAULT_NEW_ALIGNMENT__) inline std::byte
  zero_size_area[__STDCPP_DEFAULT_NEW_ALIGNMENT__];

} // namespace detail

namespace mimalloc {

class allocator final : public polymorphic_allocator {
public:
  constexpr explicit allocator(struct stats* stats) : stats_{stats} {
  }

  [[nodiscard, gnu::hot, gnu::always_inline, gnu::malloc, gnu::alloc_size(2)]]
  auto allocate(std::size_t size) noexcept -> void* final override {
    if (size == 0) {
      return &detail::zero_size_area;
    }
    auto* const ptr = ::mi_malloc(size);
    if (ptr == nullptr) {
      return nullptr;
    }
    if (stats_) {
      stats_->note_allocation(::mi_malloc_usable_size(ptr));
    }
    return ptr;
  }

  [[nodiscard, gnu::hot, gnu::always_inline, gnu::malloc, gnu::alloc_size(2),
    gnu::alloc_align(3)]]
  auto allocate(std::size_t size, std::align_val_t alignment) noexcept
    -> void* final override {
    if (size == 0) {
      return &detail::zero_size_area;
    }
    auto* const ptr = ::mi_malloc_aligned(size, std::to_underlying(alignment));
    if (ptr == nullptr) {
      return &detail::zero_size_area;
    }
    if (stats_) {
      stats_->note_allocation(::mi_malloc_usable_size(ptr));
    }
    return ptr;
  }

  [[nodiscard, gnu::hot, gnu::always_inline, gnu::malloc,
    gnu::alloc_size(2, 3)]]
  auto calloc(std::size_t count, std::size_t size) noexcept
    -> void* final override {
    if (size * count == 0) {
      return &detail::zero_size_area;
    }
    auto* const ptr = ::mi_calloc(count, size);
    if (ptr == nullptr) {
      return &detail::zero_size_area;
    }
    if (stats_) {
      stats_->note_allocation(::mi_malloc_usable_size(ptr));
    }
    return ptr;
  }

  [[nodiscard, gnu::hot, gnu::always_inline, gnu::malloc, gnu::alloc_size(2, 3),
    gnu::alloc_align(4)]]
  auto calloc(std::size_t count, std::size_t size,
              std::align_val_t alignment) noexcept -> void* final override {
    if (size * count == 0) {
      return &detail::zero_size_area;
    }
    auto* const ptr
      = ::mi_calloc_aligned(count, size, std::to_underlying(alignment));
    if (ptr == nullptr) {
      return nullptr;
    }
    if (stats_) {
      stats_->note_allocation(::mi_malloc_usable_size(ptr));
    }
    return ptr;
  }

  [[nodiscard, gnu::hot, gnu::always_inline, gnu::malloc,
    gnu::alloc_size(3)]] auto
  reallocate(void* old_ptr, std::size_t new_size) noexcept
    -> void* final override {
    if (new_size == 0) {
      deallocate(old_ptr);
      return &detail::zero_size_area;
    }
    if (old_ptr == &detail::zero_size_area) {
      return allocate(new_size);
    }
    const auto old_size = ::mi_malloc_usable_size(old_ptr);
    if (old_size >= new_size) {
      return old_ptr;
    }
    void* const new_ptr = ::mi_realloc(old_ptr, new_size);
    if (new_ptr == nullptr) {
      return nullptr;
    }
    const auto actual_new_size = ::mi_malloc_usable_size(new_ptr);
    if (stats_) {
      stats_->note_reallocation(old_ptr != new_ptr, old_size, actual_new_size);
    }
    return new_ptr;
  }

  [[nodiscard, gnu::hot, gnu::always_inline, gnu::malloc, gnu::alloc_size(3),
    gnu::alloc_align(4)]] auto
  reallocate(void* old_ptr, std::size_t new_size,
             std::align_val_t alignment) noexcept -> void* final override {
    if (new_size == 0) {
      deallocate(old_ptr);
      return &detail::zero_size_area;
    }
    if (old_ptr == &detail::zero_size_area) {
      return allocate(new_size, alignment);
    }
    const auto old_size = ::mi_malloc_usable_size(old_ptr);
    if (old_size >= new_size) {
      return old_ptr;
    }
    void* const new_ptr
      = ::mi_realloc_aligned(old_ptr, new_size, std::to_underlying(alignment));
    if (new_ptr == nullptr) {
      return nullptr;
    }
    const auto actual_new_size = ::mi_malloc_usable_size(new_ptr);
    if (stats_) {
      stats_->note_reallocation(old_ptr != new_ptr, old_size, actual_new_size);
    }
    return new_ptr;
  }

  [[gnu::hot, gnu::always_inline]]
  auto deallocate(void* ptr) noexcept -> void final override {
    if (ptr == nullptr) {
      return;
    }
    if (ptr == &detail::zero_size_area) {
      return;
    }
    if (stats_) {
      stats_->note_deallocation(::mi_malloc_usable_size(ptr));
    }
    ::mi_free(ptr);
  }

  [[gnu::hot, gnu::always_inline]]
  auto size(const void* ptr) const noexcept -> std::size_t final override {
    return ::mi_malloc_usable_size(ptr);
  }

  auto trim() noexcept -> void final override {
    ::mi_collect(false);
  }

  [[nodiscard]]
  auto stats() noexcept -> const struct stats& final override {
    return stats_ ? *stats_ : detail::zero_stats;
  }

  [[nodiscard]]
  auto backend() const noexcept -> std::string_view final override {
    return "mimalloc";
  }

private:
  struct stats* const stats_{nullptr};
};

} // namespace mimalloc

namespace system {

[[gnu::hot]]
auto native_free(void* ptr) noexcept -> void;

#ifndef __clang__
[[nodiscard, gnu::hot, gnu::malloc(native_free), gnu::alloc_size(1)]]
#else
[[nodiscard, gnu::hot, gnu::malloc, gnu::alloc_size(1)]]
#endif
auto native_malloc(std::size_t size) noexcept -> void*;

#ifndef __clang__
[[nodiscard, gnu::hot, gnu::malloc(native_free), gnu::alloc_size(1, 2)]]
#else
[[nodiscard, gnu::hot, gnu::malloc, gnu::alloc_size(1, 2)]]
#endif
auto native_calloc(std::size_t count, std::size_t size) noexcept -> void*;

#ifndef __clang__
[[nodiscard, gnu::hot, gnu::malloc(native_free), gnu::alloc_size(2)]]
#else
[[nodiscard, gnu::hot, gnu::malloc, gnu::alloc_size(2)]]
#endif
auto native_realloc(void* ptr, std::size_t new_size) noexcept -> void*;

[[nodiscard, gnu::hot]]
auto native_malloc_usable_size(const void* ptr) noexcept -> std::size_t;

#ifndef __clang__
[[nodiscard, gnu::hot, gnu::malloc(native_free), gnu::alloc_size(1),
  gnu::alloc_align(2)]]
#else
[[nodiscard, gnu::hot, gnu::malloc, gnu::alloc_size(1), gnu::alloc_align(2)]]
#endif
auto malloc_aligned(std::size_t size, std::size_t alignment) noexcept -> void*;

#ifndef __clang__
[[nodiscard, gnu::hot, gnu::malloc(native_free), gnu::alloc_size(2),
  gnu::alloc_align(3)]]
#else
[[nodiscard, gnu::hot, gnu::malloc, gnu::alloc_size(2), gnu::alloc_align(3)]]
#endif
auto realloc_aligned(void* ptr, std::size_t new_size,
                     std::size_t alignment) noexcept -> void*;

#ifndef __clang__
[[nodiscard, gnu::hot, gnu::malloc(native_free), gnu::alloc_size(1, 2),
  gnu::alloc_align(3)]]
#else
[[nodiscard, gnu::hot, gnu::malloc, gnu::alloc_size(1, 2), gnu::alloc_align(3)]]
#endif
auto calloc_aligned(std::size_t count, std::size_t size,
                    std::size_t alignment) noexcept -> void*;

[[nodiscard, gnu::hot]] auto native_malloc_size(const void*) noexcept
  -> std::size_t;
[[gnu::hot]] auto trim() noexcept -> void;

class allocator final : public polymorphic_allocator {
public:
  constexpr explicit allocator(struct stats* stats) : stats_{stats} {
  }

  [[nodiscard, gnu::hot, gnu::always_inline, gnu::always_inline, gnu::malloc,
    gnu::alloc_size(2)]]
  auto allocate(std::size_t size) noexcept -> void* final override {
    if (size == 0) {
      return &detail::zero_size_area;
    }
    auto* const ptr = system::native_malloc(size);
    if (ptr == nullptr) {
      return nullptr;
    }
    if (stats_) {
      stats_->note_allocation(system::native_malloc_size(ptr));
    }
    return ptr;
  }

  [[nodiscard, gnu::hot, gnu::always_inline, gnu::malloc, gnu::alloc_size(2),
    gnu::alloc_align(3)]]
  auto allocate(std::size_t size, std::align_val_t alignment) noexcept
    -> void* final override {
    if (size == 0) {
      return &detail::zero_size_area;
    }
    auto* const ptr
      = system::malloc_aligned(size, std::to_underlying(alignment));
    if (ptr == nullptr) {
      return nullptr;
    }
    if (stats_) {
      stats_->note_allocation(system::native_malloc_size(ptr));
    }
    return ptr;
  }

  [[nodiscard, gnu::hot, gnu::always_inline, gnu::malloc,
    gnu::alloc_size(2, 3)]]
  auto calloc(std::size_t count, std::size_t size) noexcept
    -> void* final override {
    if (size * count == 0) {
      return &detail::zero_size_area;
    }
    auto* const ptr = system::native_calloc(count, size);
    if (ptr == nullptr) {
      return nullptr;
    }
    if (stats_) {
      stats_->note_allocation(system::native_malloc_size(ptr));
    }
    return ptr;
  }

  [[nodiscard, gnu::hot, gnu::always_inline, gnu::malloc, gnu::alloc_size(2, 3),
    gnu::alloc_align(4)]]
  auto calloc(std::size_t count, std::size_t size,
              std::align_val_t alignment) noexcept -> void* final override {
    if (size * count == 0) {
      return &detail::zero_size_area;
    }

    auto* const ptr
      = system::calloc_aligned(count, size, std::to_underlying(alignment));
    if (ptr == nullptr) {
      return nullptr;
    }
    if (stats_) {
      stats_->note_allocation(system::native_malloc_size(ptr));
    }
    return ptr;
  }

  [[nodiscard, gnu::hot, gnu::always_inline, gnu::malloc, gnu::alloc_size(3)]]
  auto reallocate(void* old_ptr, std::size_t new_size) noexcept
    -> void* final override {
    if (new_size == 0) {
      deallocate(old_ptr);
      return &detail::zero_size_area;
    }
    if (old_ptr == &detail::zero_size_area) {
      return allocate(new_size);
    }
    const auto old_size = system::native_malloc_size(old_ptr);
    if (old_size >= new_size) {
      return old_ptr;
    }
    void* const new_ptr = system::native_realloc(old_ptr, new_size);
    if (new_ptr == nullptr) {
      return nullptr;
    }
    const auto actual_new_size = system::native_malloc_size(new_ptr);
    if (stats_) {
      stats_->note_reallocation(old_ptr != new_ptr, old_size, actual_new_size);
    }
    return new_ptr;
  }

  [[nodiscard, gnu::hot, gnu::malloc, gnu::alloc_size(3), gnu::alloc_align(4)]]
  auto reallocate(void* old_ptr, std::size_t new_size,
                  std::align_val_t alignment) noexcept -> void* final override {
    if (new_size == 0) {
      deallocate(old_ptr);
      return &detail::zero_size_area;
    }
    if (old_ptr == &detail::zero_size_area) {
      return allocate(new_size, alignment);
    }
    const auto old_size = system::native_malloc_size(old_ptr);
    if (old_size >= new_size) {
      return old_ptr;
    }
    void* const new_ptr = system::realloc_aligned(
      old_ptr, new_size, std::to_underlying(alignment));
    if (new_ptr == nullptr) {
      return nullptr;
    }
    const auto actual_new_size = system::native_malloc_size(new_ptr);
    if (stats_) {
      stats_->note_reallocation(old_ptr != new_ptr, old_size, actual_new_size);
    }
    return new_ptr;
  }

  [[gnu::hot, gnu::always_inline]]
  auto deallocate(void* ptr) noexcept -> void final override {
    if (ptr == nullptr) {
      return;
    }
    if (ptr == &detail::zero_size_area) {
      return;
    }
    if (stats_) {
      stats_->note_deallocation(system::native_malloc_size(ptr));
    }
    system::native_free(ptr);
  }

  virtual auto size(const void* ptr) const noexcept
    -> std::size_t final override {
    return system::native_malloc_size(ptr);
  }

  auto trim() noexcept -> void final override {
    system::trim();
  }

  [[nodiscard]]
  auto stats() noexcept -> const struct stats& final override {
    return stats_ ? *stats_ : detail::zero_stats;
  }

  [[nodiscard]] auto backend() const noexcept
    -> std::string_view final override {
    return "system";
  }

private:
  struct stats* const stats_{nullptr};
};

} // namespace system

enum class selected_alloc {
  mimalloc,
  system,
};

[[gnu::const]]
auto selected_alloc(const char* env) noexcept -> selected_alloc;

[[gnu::const]]
auto enable_stats(const char* env) noexcept -> bool;

[[gnu::const]]
auto trim_interval() noexcept -> tenzir::duration;

#if TENZIR_SELECT_ALLOCATOR == TENZIR_SELECT_ALLOCATOR_RUNTIME

#  define TENZIR_MAKE_ALLOCATOR(NAME, ENV_SUFFIX)                              \
    [[nodiscard, gnu::hot, gnu::const]] inline auto NAME() noexcept            \
      -> polymorphic_allocator& {                                              \
      constinit static auto stats_ = stats{};                                  \
      static auto mimalloc_ = mimalloc::allocator{                             \
        enable_stats("TENZIR_ALLOC_STATS_" ENV_SUFFIX) ? &stats_ : nullptr};   \
      static auto system_ = system::allocator{                                 \
        enable_stats("TENZIR_ALLOC_STATS_" ENV_SUFFIX) ? &stats_ : nullptr};   \
      static auto& instance                                                    \
        = selected_alloc("TENZIR_ALLOC_" ENV_SUFFIX)                           \
              == selected_alloc::mimalloc                                      \
            ? static_cast<polymorphic_allocator&>(mimalloc_)                   \
            : static_cast<polymorphic_allocator&>(system_);                    \
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
TENZIR_MAKE_ALLOCATOR(arrow_allocator, "TENZIR_ALLOC_ARROW")
/// The allocator used by `operator new` and `operator delete`.
TENZIR_MAKE_ALLOCATOR(cpp_allocator, "TENZIR_ALLOC_CPP")
/// The allocator used by `malloc` and other C/POSIX allocation functions.
TENZIR_MAKE_ALLOCATOR(c_allocator, "TENZIR_ALLOC_C")

#undef TENZIR_MAKE_ALLOCATOR

} // namespace tenzir::memory

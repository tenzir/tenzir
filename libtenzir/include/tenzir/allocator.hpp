//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

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

using name_function_t = auto (*)() noexcept -> std::string_view;
using alloc_function_t = auto (*)(std::size_t) noexcept -> void*;
using alloc_aligned_function_t
  = auto (*)(std::size_t, std::size_t) noexcept -> void*;
using calloc_function_t = auto (*)(std::size_t, std::size_t) noexcept -> void*;
using calloc_aligned_function_t
  = auto (*)(std::size_t, std::size_t, std::size_t) noexcept -> void*;
using realloc_function_t = auto (*)(void*, std::size_t) noexcept -> void*;
using realloc_aligned_function_t
  = auto (*)(void*, std::size_t, std::size_t) noexcept -> void*;
using dealloc_function_t = auto (*)(void*) noexcept -> void;
using trim_function_t = auto (*)() noexcept -> void;
using size_function_t = auto (*)(const void*) noexcept -> std::size_t;

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
  virtual auto trim() noexcept -> void = 0;
  virtual auto stats() noexcept -> const struct stats& = 0;
  virtual auto backend() const noexcept -> std::string_view = 0;
  [[nodiscard, gnu::hot]]
  virtual auto size(const void*) const noexcept -> std::size_t
    = 0;
};

namespace detail {

inline constinit auto zero_stats = stats{};

struct allocator_configuration {
  const name_function_t name;
  std::align_val_t default_alignment;
  const alloc_function_t alloc;
  const alloc_aligned_function_t alloc_aligned;
  const calloc_function_t calloc;
  const calloc_aligned_function_t calloc_aligned;
  const realloc_function_t realloc;
  const realloc_aligned_function_t realloc_aligned;
  const dealloc_function_t dealloc;
  const size_function_t size;
  const trim_function_t trim;
};

template <allocator_configuration config>
class allocator_impl final : public polymorphic_allocator {
public:
  constexpr static auto default_alignment = config.default_alignment;
  constexpr allocator_impl(struct stats* stats) : stats_{stats} {
  }
  [[nodiscard, gnu::hot, gnu::malloc, gnu::alloc_size(2)]]
  virtual auto allocate(std::size_t) noexcept -> void* override;
  [[nodiscard, gnu::hot, gnu::malloc, gnu::alloc_size(2), gnu::alloc_align(3)]]
  virtual auto allocate(std::size_t, std::align_val_t) noexcept
    -> void* override;
  [[nodiscard, gnu::hot, gnu::malloc, gnu::alloc_size(2, 3)]]
  virtual auto calloc(std::size_t, std::size_t) noexcept -> void* override;
  [[nodiscard, gnu::hot, gnu::malloc, gnu::alloc_size(2, 3),
    gnu::alloc_align(4)]]
  virtual auto calloc(std::size_t, std::size_t, std::align_val_t) noexcept
    -> void* override;
  [[nodiscard, gnu::hot, gnu::malloc, gnu::alloc_size(3)]]
  virtual auto reallocate(void*, std::size_t) noexcept -> void* override;
  [[nodiscard, gnu::hot, gnu::malloc, gnu::alloc_size(3), gnu::alloc_align(4)]]
  virtual auto reallocate(void*, std::size_t, std::align_val_t) noexcept
    -> void* override;
  virtual auto deallocate(void*) noexcept -> void override;
  virtual auto trim() noexcept -> void override {
    config.trim();
  }
  virtual auto stats() noexcept -> const struct stats& override {
    return stats_ ? *stats_ : zero_stats;
  }
  virtual auto backend() const noexcept -> std::string_view override {
    return config.name();
  }
  [[nodiscard, gnu::hot]]
  virtual auto size(const void* ptr) const noexcept -> std::size_t override {
    if (! ptr) {
      return 0;
    }
    return config.size(ptr);
  }

private:
  struct stats* const stats_;
};

} // namespace detail

namespace mimalloc {

auto trim() noexcept -> void;
inline auto name() noexcept -> std::string_view {
  return "mimalloc";
}

constexpr static auto config = detail::allocator_configuration{
  .name = name,
  .default_alignment = std::align_val_t{16},
  .alloc = ::mi_malloc,
  .alloc_aligned = ::mi_malloc_aligned,
  .calloc = ::mi_calloc,
  .calloc_aligned = ::mi_calloc_aligned,
  .realloc = ::mi_realloc,
  .realloc_aligned = ::mi_realloc_aligned,
  .dealloc = ::mi_free,
  .size = ::mi_malloc_usable_size,
  .trim = mimalloc::trim,
};
using polymorphic_allocator = detail::allocator_impl<config>;

struct static_allocator {
  constexpr explicit static_allocator(struct stats* stats) : stats_{stats} {
  }

  [[nodiscard, gnu::hot, gnu::always_inline, gnu::always_inline, gnu::malloc,
    gnu::alloc_size(2)]]
  auto allocate(std::size_t size) noexcept -> void* {
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
    -> void* {
    auto* const ptr = ::mi_malloc_aligned(size, std::to_underlying(alignment));
    if (ptr == nullptr) {
      return nullptr;
    }
    if (stats_) {
      stats_->note_allocation(::mi_malloc_usable_size(ptr));
    }
    return ptr;
  }

  [[nodiscard, gnu::hot, gnu::always_inline, gnu::malloc,
    gnu::alloc_size(2, 3)]]
  auto calloc(std::size_t count, std::size_t size) noexcept -> void* {
    auto* const ptr = ::mi_calloc(count, size);
    if (ptr == nullptr) {
      return nullptr;
    }
    if (stats_) {
      stats_->note_allocation(::mi_malloc_usable_size(ptr));
    }
    return ptr;
  }

  [[nodiscard, gnu::hot, gnu::always_inline, gnu::malloc, gnu::alloc_size(2, 3),
    gnu::alloc_align(4)]]
  auto calloc(std::size_t count, std::size_t size,
              std::align_val_t alignment) noexcept -> void* {
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

  [[nodiscard, gnu::hot, gnu::always_inline, gnu::malloc, gnu::alloc_size(3)]]
  auto reallocate(void* old_ptr, std::size_t new_size) noexcept -> void* {
    if (new_size == 0) {
      deallocate(old_ptr);
      return nullptr;
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

  [[nodiscard, gnu::hot, gnu::malloc, gnu::alloc_size(3), gnu::alloc_align(4)]]
  auto reallocate(void* old_ptr, std::size_t new_size,
                  std::align_val_t alignment) noexcept -> void* {
    if (new_size == 0) {
      deallocate(old_ptr);
      return nullptr;
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
  auto deallocate(void* ptr) noexcept -> void {
    if (ptr == nullptr) {
      return;
    }
    if (stats_) {
      stats_->note_deallocation(::mi_malloc_usable_size(ptr));
    }
    ::mi_free(ptr);
  }

  auto trim() noexcept -> void {
    ::mi_collect(false);
  }

  auto stats() noexcept -> const struct stats& {
    return stats_ ? *stats_ : detail::zero_stats;
  }

  auto backend() noexcept -> std::string_view {
    return "mimalloc";
  }

  [[nodiscard, gnu::hot]]
  auto size(const void* ptr) noexcept -> std::size_t {
    if (! ptr) {
      return 0;
    }
    return ::mi_malloc_usable_size(ptr);
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

#ifndef __clang__
[[nodiscard, gnu::hot, gnu::malloc(native_free), gnu::alloc_size(2, 3)]]
#else
[[nodiscard, gnu::hot, gnu::malloc, gnu::alloc_size(2, 3)]]
#endif
auto native_reallocarray(void* ptr, std::size_t count,
                         std::size_t size) noexcept -> void*;

#ifndef __clang__
[[nodiscard, gnu::hot, gnu::malloc(native_free), gnu::alloc_size(2),
  gnu::alloc_align(1)]]
#else
[[nodiscard, gnu::hot, gnu::malloc, gnu::alloc_size(2), gnu::alloc_align(1)]]
#endif
auto native_memalign(std::size_t alignment, std::size_t size) noexcept -> void*;

#ifndef __clang__
[[nodiscard, gnu::hot, gnu::malloc(native_free), gnu::alloc_size(2),
  gnu::alloc_align(1)]]
#else
[[nodiscard, gnu::hot, gnu::malloc, gnu::alloc_size(2), gnu::alloc_align(1)]]
#endif
auto native_aligned_alloc(std::size_t alignment, std::size_t size) noexcept
  -> void*;

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

[[nodiscard, gnu::hot]] auto malloc_size(const void*) noexcept -> std::size_t;
[[gnu::hot]] auto trim() noexcept -> void;

inline auto name() noexcept -> std::string_view {
  return "system";
}

constexpr static auto config = detail::allocator_configuration{
  .name = name,
  .default_alignment = std::align_val_t{16},
  .alloc = system::native_malloc,
  .alloc_aligned = system::malloc_aligned,
  .calloc = system::native_calloc,
  .calloc_aligned = system::calloc_aligned,
  .realloc = system::native_realloc,
  .realloc_aligned = system::realloc_aligned,
  .dealloc = system::native_free,
  .size = system::malloc_size,
  .trim = system::trim,
};
using polymorphic_allocator = detail::allocator_impl<config>;

struct static_allocator {
  constexpr explicit static_allocator(struct stats* stats) : stats_{stats} {
  }

  [[nodiscard, gnu::hot, gnu::always_inline, gnu::always_inline, gnu::malloc,
    gnu::alloc_size(2)]]
  auto allocate(std::size_t size) noexcept -> void* {
    auto* const ptr = std::malloc(size);
    if (ptr == nullptr) {
      return nullptr;
    }
    if (stats_) {
      stats_->note_allocation(system::malloc_size(ptr));
    }
    return ptr;
  }

  [[nodiscard, gnu::hot, gnu::always_inline, gnu::malloc, gnu::alloc_size(2),
    gnu::alloc_align(3)]]
  auto allocate(std::size_t size, std::align_val_t alignment) noexcept
    -> void* {
    auto* const ptr
      = system::malloc_aligned(size, std::to_underlying(alignment));
    if (ptr == nullptr) {
      return nullptr;
    }
    if (stats_) {
      stats_->note_allocation(system::malloc_size(ptr));
    }
    return ptr;
  }

  [[nodiscard, gnu::hot, gnu::always_inline, gnu::malloc,
    gnu::alloc_size(2, 3)]]
  auto calloc(std::size_t count, std::size_t size) noexcept -> void* {
    auto* const ptr = std::calloc(count, size);
    if (ptr == nullptr) {
      return nullptr;
    }
    if (stats_) {
      stats_->note_allocation(system::malloc_size(ptr));
    }
    return ptr;
  }

  [[nodiscard, gnu::hot, gnu::always_inline, gnu::malloc, gnu::alloc_size(2, 3),
    gnu::alloc_align(4)]]
  auto calloc(std::size_t count, std::size_t size,
              std::align_val_t alignment) noexcept -> void* {
    auto* const ptr
      = system::calloc_aligned(count, size, std::to_underlying(alignment));
    if (ptr == nullptr) {
      return nullptr;
    }
    if (stats_) {
      stats_->note_allocation(system::malloc_size(ptr));
    }
    return ptr;
  }

  [[nodiscard, gnu::hot, gnu::always_inline, gnu::malloc, gnu::alloc_size(3)]]
  auto reallocate(void* old_ptr, std::size_t new_size) noexcept -> void* {
    if (new_size == 0) {
      deallocate(old_ptr);
      return nullptr;
    }
    const auto old_size = system::malloc_size(old_ptr);
    if (old_size >= new_size) {
      return old_ptr;
    }
    void* const new_ptr = std::realloc(old_ptr, new_size);
    if (new_ptr == nullptr) {
      return nullptr;
    }
    const auto actual_new_size = system::malloc_size(new_ptr);
    if (stats_) {
      stats_->note_reallocation(old_ptr != new_ptr, old_size, actual_new_size);
    }
    return new_ptr;
  }

  [[nodiscard, gnu::hot, gnu::malloc, gnu::alloc_size(3), gnu::alloc_align(4)]]
  auto reallocate(void* old_ptr, std::size_t new_size,
                  std::align_val_t alignment) noexcept -> void* {
    if (new_size == 0) {
      deallocate(old_ptr);
      return nullptr;
    }
    const auto old_size = system::malloc_size(old_ptr);
    if (old_size >= new_size) {
      return old_ptr;
    }
    void* const new_ptr = system::realloc_aligned(
      old_ptr, new_size, std::to_underlying(alignment));
    if (new_ptr == nullptr) {
      return nullptr;
    }
    const auto actual_new_size = system::malloc_size(new_ptr);
    if (stats_) {
      stats_->note_reallocation(old_ptr != new_ptr, old_size, actual_new_size);
    }
    return new_ptr;
  }

  [[gnu::hot, gnu::always_inline]]
  auto deallocate(void* ptr) noexcept -> void {
    if (ptr == nullptr) {
      return;
    }
    if (stats_) {
      stats_->note_deallocation(system::malloc_size(ptr));
    }
    std::free(ptr);
  }

  auto trim() noexcept -> void {
    system::trim();
  }

  auto stats() noexcept -> const struct stats& {
    return stats_ ? *stats_ : detail::zero_stats;
  }

  auto backend() noexcept -> std::string_view {
    return "system";
  }

  [[nodiscard, gnu::hot]]
  auto size(const void* ptr) noexcept -> std::size_t {
    if (! ptr) {
      return 0;
    }
    return system::malloc_size(ptr);
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
auto selected_alloc(const char*) noexcept -> selected_alloc;

[[gnu::const]]
auto enable_stats(const char*) noexcept -> bool;

#if TENZIR_SELECT_ALLOCATOR == TENZIR_SELECT_ALLOCATOR_RUNTIME

#  define TENZIR_MAKE_ALLOCATOR(NAME, ENV_NAME)                                \
    [[nodiscard, gnu::hot, gnu::const]] inline auto NAME() noexcept            \
      -> polymorphic_allocator& {                                              \
      constinit static auto stats_ = stats{};                                  \
      constinit static auto mimalloc_stats_                                    \
        = mimalloc::polymorphic_allocator{&stats_};                            \
      constinit static auto mimalloc_                                          \
        = mimalloc::polymorphic_allocator{nullptr};                            \
      constinit static auto system_stats_                                      \
        = system::polymorphic_allocator{&stats_};                              \
      constinit static auto system_ = system::polymorphic_allocator{nullptr};  \
      static auto& instance                                                    \
        = selected_alloc(ENV_NAME) == selected_alloc::mimalloc                 \
            ? static_cast<polymorphic_allocator&>(                             \
                enable_stats(ENV_NAME "_STATS") ? mimalloc_stats_ : mimalloc_) \
            : static_cast<polymorphic_allocator&>(                             \
                enable_stats(ENV_NAME "_STATS") ? system_stats_ : system_);    \
      return instance;                                                         \
    }

#elif TENZIR_SELECT_ALLOCATOR == TENZIR_SELECT_ALLOCATOR_MIMALLOC

#  define TENZIR_MAKE_ALLOCATOR(NAME, ENV_NAME)                                \
    [[nodiscard, gnu::hot, gnu::const]] inline auto NAME() noexcept            \
      -> mimalloc::static_allocator& {                                         \
      constinit static auto stats_ = stats{};                                  \
      static auto instance = mimalloc::static_allocator{                       \
        enable_stats(ENV_NAME "") ? &stats_ : nullptr};                        \
      return instance;                                                         \
    }

#elif TENZIR_SELECT_ALLOCATOR == TENZIR_SELECT_ALLOCATOR_SYSTEM

#  define TENZIR_MAKE_ALLOCATOR(NAME, ENV_NAME)                                \
    [[nodiscard, gnu::hot, gnu::const]] inline auto NAME() noexcept            \
      -> system::static_allocator& {                                           \
      constinit static auto stats_ = stats{};                                  \
      static auto instance = system::static_allocator{                         \
        enable_stats(ENV_NAME "") ? &stats_ : nullptr};                        \
      return instance;                                                         \
    }

#else

struct dummy_allocator {
  static auto stats() noexcept -> const stats& {
    return detail::zero_stats;
  }
};

#  define TENZIR_MAKE_ALLOCATOR(NAME, ENV_NAME)                                \
    [[nodiscard]] inline auto NAME() noexcept -> dummy_allocator& {            \
      constinit static auto instance = dummy_allocator{};                      \
      return instance;                                                         \
    }
#endif

TENZIR_MAKE_ALLOCATOR(arrow_allocator, "TENZIR_ALLOC_ARROW")
TENZIR_MAKE_ALLOCATOR(cpp_allocator, "TENZIR_ALLOC_CPP")
TENZIR_MAKE_ALLOCATOR(c_allocator, "TENZIR_ALLOC_C")

#undef TENZIR_MAKE_ALLOCATOR

} // namespace tenzir::memory

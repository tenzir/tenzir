//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tsl/robin_map.h>

#include <atomic>
#include <concepts>
#include <cstddef>
#include <mimalloc.h>
#include <new>

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
concept allocator
  = requires(T t, std::size_t size, std::align_val_t alignment, void* ptr) {
      { t.allocate(size) } noexcept -> std::same_as<void*>;
      { t.allocate(size, alignment) } noexcept -> std::same_as<void*>;
      { t.reallocate(ptr, size) } noexcept -> std::same_as<void*>;
      { t.reallocate(ptr, size, alignment) } noexcept -> std::same_as<void*>;
      { t.deallocate(ptr) } noexcept -> std::same_as<void>;
      { t.trim() } noexcept;
      { t.stats() } noexcept -> std::same_as<const stats&>;
      { t.backend() } noexcept -> std::same_as<std::string_view>;
    };

using name_function_t = auto (*)() noexcept -> std::string_view;
using alloc_function_t = auto (*)(std::size_t) noexcept -> void*;
using alloc_aligned_function_t
  = auto (*)(std::size_t, std::size_t) noexcept -> void*;
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
  [[nodiscard, gnu::hot, gnu::alloc_size(3)]]
  virtual auto reallocate(void*, std::size_t) noexcept -> void* = 0;
  [[nodiscard, gnu::hot, gnu::alloc_size(3), gnu::alloc_align(4)]]
  virtual auto reallocate(void*, std::size_t, std::align_val_t) noexcept
    -> void* = 0;
  virtual auto deallocate(void*) noexcept -> void = 0;
  virtual auto trim() noexcept -> void = 0;
  virtual auto stats() noexcept -> const struct stats& = 0;
  virtual auto backend() const noexcept -> std::string_view = 0;
};

namespace detail {

inline constinit auto zero_stats = stats{};

struct allocator_configuration {
  const name_function_t name;
  std::align_val_t default_alignment;
  const alloc_function_t alloc;
  const alloc_aligned_function_t alloc_aligned;
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
  .realloc = ::mi_realloc,
  .realloc_aligned = ::mi_realloc_aligned,
  .dealloc = ::mi_free,
  .size = ::mi_malloc_usable_size,
  .trim = mimalloc::trim,
};
using allocator = detail::allocator_impl<config>;
} // namespace mimalloc

namespace system {

#ifndef __clang__
[[nodiscard, gnu::hot, gnu::malloc(std::free), gnu::alloc_size(1),
  gnu::alloc_align(2)]]
#else
[[nodiscard, gnu::hot, gnu::malloc, gnu::alloc_size(1), gnu::alloc_align(2)]]
#endif
auto malloc_aligned(std::size_t size, std::size_t alignment) noexcept -> void*;

#ifndef __clang__
[[nodiscard, gnu::hot, gnu::malloc(std::free), gnu::alloc_size(2),
  gnu::alloc_align(3)]]
#else
[[nodiscard, gnu::hot, gnu::malloc, gnu::alloc_size(2), gnu::alloc_align(3)]]
#endif
auto realloc_aligned(void* ptr, std::size_t new_size,
                     std::size_t alignment) noexcept -> void*;

[[nodiscard, gnu::hot]] auto malloc_size(const void*) noexcept -> std::size_t;
[[gnu::hot]] auto trim() noexcept -> void;

inline auto name() noexcept -> std::string_view {
  return "system";
}

constexpr static auto config = detail::allocator_configuration{
  .name = name,
  .default_alignment = std::align_val_t{16},
  .alloc = static_cast<alloc_function_t>(std::malloc),
  .alloc_aligned = system::malloc_aligned,
  .realloc = static_cast<realloc_function_t>(std::realloc),
  .realloc_aligned = system::realloc_aligned,
  .dealloc = static_cast<dealloc_function_t>(std::free),
  .size = system::malloc_size,
  .trim = trim,
};
using allocator = detail::allocator_impl<config>;
} // namespace system

enum class selected_alloc {
  mimalloc,
  system,
};

[[gnu::const]]
auto selected_alloc(const char*) noexcept -> selected_alloc;

[[gnu::const]]
auto enable_stats(const char*) noexcept -> bool;

// The allocation wrapper used by the arrow memory pool
[[nodiscard, gnu::hot, gnu::const]] inline auto arrow_allocator() noexcept
  -> polymorphic_allocator& {
  constinit static auto stats_ = stats{};
  constinit static auto mimalloc_stats_ = mimalloc::allocator{&stats_};
  constinit static auto mimalloc_ = mimalloc::allocator{nullptr};
  constinit static auto system_stats_ = system::allocator{&stats_};
  constinit static auto system_ = system::allocator{nullptr};
  static auto& instance
    = selected_alloc("TENZIR_ALLOC_ARROW") == selected_alloc::mimalloc
        ? static_cast<polymorphic_allocator&>(
            enable_stats("TENZIR_ALLOC_ARROW_STATS") ? mimalloc_stats_
                                                     : mimalloc_)
        : static_cast<polymorphic_allocator&>(
            enable_stats("TENZIR_ALLOC_ARROW_STATS") ? system_stats_ : system_);
  return instance;
}

// The allocation wrapper used by operator new/delete
[[nodiscard, gnu::hot, gnu::const]] inline auto cpp_allocator() noexcept
  -> polymorphic_allocator& {
  constinit static auto stats_ = stats{};
  constinit static auto mimalloc_stats_ = mimalloc::allocator{&stats_};
  constinit static auto mimalloc_ = mimalloc::allocator{nullptr};
  constinit static auto system_stats_ = system::allocator{&stats_};
  constinit static auto system_ = system::allocator{nullptr};
  static auto& instance
    = selected_alloc("TENZIR_ALLOC_ARROW") == selected_alloc::mimalloc
        ? static_cast<polymorphic_allocator&>(
            enable_stats("TENZIR_ALLOC_ARROW_STATS") ? mimalloc_stats_
                                                     : mimalloc_)
        : static_cast<polymorphic_allocator&>(
            enable_stats("TENZIR_ALLOC_ARROW_STATS") ? system_stats_ : system_);
  return instance;
}

} // namespace tenzir::memory

//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/detail/string_literal.hpp"

#include <tsl/robin_map.h>

#include <atomic>
#include <concepts>
#include <cstddef>
#include <mimalloc.h>
#include <new>

namespace tenzir::memory {

struct stats {
  std::atomic<std::int64_t> bytes_current;
  std::atomic<std::int64_t> bytes_total;
  std::atomic<std::int64_t> bytes_max;
  std::atomic<std::int64_t> num_calls;
  std::atomic<std::int64_t> allocations_current;
  std::atomic<std::int64_t> allocations_total;
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

[[nodiscard, gnu::hot, gnu::malloc(std::free), gnu::alloc_size(1),
  gnu::alloc_align(2)]]
auto malloc_aligned(std::size_t size, std::size_t alignment) noexcept -> void*;

[[nodiscard, gnu::hot, gnu::malloc(std::free), gnu::alloc_size(2),
  gnu::alloc_align(3)]]
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
  .alloc = std::malloc,
  .alloc_aligned = system::malloc_aligned,
  .realloc = std::realloc,
  .realloc_aligned = system::realloc_aligned,
  .dealloc = std::free,
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
  static auto mimalloc_ = mimalloc::allocator{
    enable_stats("TENZIR_ALLOC_ARROW_STATS") ? &stats_ : nullptr};
  static auto system_ = system::allocator{
    enable_stats("TENZIR_ALLOC_ARROW_STATS") ? &stats_ : nullptr};
  static auto& instance
    = selected_alloc("TENZIR_ALLOC_ARROW") == selected_alloc::mimalloc
        ? static_cast<polymorphic_allocator&>(mimalloc_)
        : system_;
  // constinit static auto instance = mimalloc::allocator{nullptr};
  return instance;
}

// The allocation wrapper used by operator new/delete
[[nodiscard, gnu::hot, gnu::const]] inline auto cpp_allocator() noexcept
  -> polymorphic_allocator& {
  constinit static auto stats_ = stats{};
  static auto mimalloc_ = mimalloc::allocator{
    enable_stats("TENZIR_ALLOC_CPP_STATS") ? &stats_ : nullptr};
  static auto system_ = system::allocator{
    enable_stats("TENZIR_ALLOC_CPP_STATS") ? &stats_ : nullptr};
  static auto& instance
    = selected_alloc("TENZIR_ALLOC_CPP") == selected_alloc::mimalloc
        ? static_cast<polymorphic_allocator&>(mimalloc_)
        : system_;
  // constinit static auto instance = mimalloc::allocator{nullptr};
  return instance;
}

} // namespace tenzir::memory

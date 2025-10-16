//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/logger.hpp"

#include <tsl/robin_map.h>

#include <atomic>
#include <concepts>
#include <cstddef>
#include <new>

namespace tenzir::memory {

struct block {
  std::byte* ptr = nullptr;
  std::size_t size = 0;

  operator bool() const {
    return ptr != nullptr;
  }
};

struct reallocation_result {
  block true_old_block;
  block new_block;
};

struct stats {
  std::atomic<std::int64_t> bytes_current;
  std::atomic<std::int64_t> bytes_total;
  std::atomic<std::int64_t> bytes_max;
  std::atomic<std::int64_t> num_calls;
  std::atomic<std::int64_t> allocations_current;
  std::atomic<std::int64_t> allocations_total;
  std::atomic<std::int64_t> allocations_max;

  auto note_allocation(std::int64_t add) noexcept -> void;
  auto note_reallocation(const reallocation_result& realloc) noexcept -> void;
  auto note_deallocation(std::int64_t remove) noexcept -> void;
  auto update_max_bytes(std::int64_t new_usage) noexcept -> void;
  auto add_allocation() noexcept -> void;
};

template <typename T>
concept allocator
  = requires(T t, std::size_t size, std::align_val_t alignment, block blk) {
      { t.allocate(size, alignment) } -> std::same_as<block>;
      {
        t.reallocate(blk, size, alignment)
      } -> std::same_as<reallocation_result>;
      { t.deallocate(blk, alignment) } -> std::same_as<std::size_t>;
      { t.backend() } -> std::same_as<std::string_view>;
    };

template <typename T>
concept allocator_with_stats = allocator<T> and requires(const T t) {
  { t.stats() } -> std::same_as<const stats&>;
};

using allocation_function_t
  = auto (*)(std::size_t size, std::align_val_t alignment) -> block;
using reallocation_function_t
  = auto (*)(block old_block, std::size_t size, std::align_val_t alignment)
    -> reallocation_result;
using deallocation_function_t
  = auto (*)(block old_block, std::align_val_t alignment) -> std::size_t;

struct erased_allocator {
  allocation_function_t allocate;
  reallocation_function_t reallocate;
  deallocation_function_t deallocate;
  std::string_view backend_;

  auto backend() const -> std::string_view {
    return backend_;
  }
};

static_assert(allocator<erased_allocator>);

template <allocator Inner>
class stats_allocator {
public:
  template <typename... Ts>
  stats_allocator(Ts&&... ts) : inner_{std::forward<Ts>(ts)...} {
  }

  auto allocate(std::size_t size, std::align_val_t alignment) noexcept
    -> block {
    auto blk = inner_.allocate(size, alignment);
    if (not blk) {
      return {};
    }
    stats_.note_allocation(blk.size);
    return blk;
  }

  auto reallocate(block blk, std::size_t new_size,
                  std::align_val_t alignment) noexcept -> reallocation_result {
    auto result = inner_.reallocate(blk, new_size, alignment);
    stats_.note_reallocation(result);
    return result;
  }

  auto deallocate(block blk, std::align_val_t alignment) noexcept
    -> std::size_t {
    auto size = inner_.deallocate(blk, alignment);
    stats_.note_deallocation(size);
    return size;
  }

  auto backend() const -> std::string_view {
    return inner_.backend();
  }

  auto stats() const -> const stats& {
    return stats_;
  }

private:
  Inner inner_;
  struct stats stats_;
};

static_assert(allocator_with_stats<stats_allocator<erased_allocator>>);

template <allocator Inner>
class wrapping_allocator {
public:
  wrapping_allocator(Inner& inner) : inner_{inner} {
  }
  auto allocate(std::size_t size, std::align_val_t alignment) noexcept
    -> block {
    return inner_.allocate(size, alignment);
  }

  auto reallocate(block blk, std::size_t new_size,
                  std::align_val_t alignment) noexcept -> reallocation_result {
    return inner_.reallocate(blk, new_size, alignment);
  }

  auto deallocate(block blk, std::align_val_t alignment) noexcept
    -> std::size_t {
    return inner_.deallocate(blk, alignment);
  }

  auto backend() const -> std::string_view {
    return inner_.backend();
  }

  auto stats() const -> const stats&
    requires allocator_with_stats<Inner>
  {
    return inner_.stats_();
  }

private:
  Inner& inner_;
};

static_assert(allocator<wrapping_allocator<erased_allocator>>);
static_assert(
  allocator_with_stats<wrapping_allocator<stats_allocator<erased_allocator>>>);

using global_allocator_t = stats_allocator<erased_allocator>;
using separated_allocator_t
  = stats_allocator<wrapping_allocator<global_allocator_t>>;

static_assert(allocator<wrapping_allocator<global_allocator_t>>);

/// Returns the global allocator instance
[[nodiscard]] auto global_allocator() noexcept -> global_allocator_t&;

// The allocation wrapper used by the arrow memory pool
[[nodiscard]] auto arrow_allocator() noexcept -> separated_allocator_t&;

// The allocation wrapper used by operator new/delete
[[nodiscard]] auto cpp_allocator() noexcept -> separated_allocator_t&;

} // namespace tenzir::memory

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
    };

template <typename T>
concept allocator_with_stats = allocator<T> and requires(const T t) {
  { t.stats() } -> std::same_as<const stats&>;
};

class mimallocator {
public:
  mimallocator() noexcept;
  auto allocate(std::size_t size, std::align_val_t alignment) noexcept -> block;

  auto reallocate(block blk, std::size_t new_size,
                  std::align_val_t alignment) noexcept -> reallocation_result;

  auto deallocate(block blk, std::align_val_t alignment) noexcept
    -> std::size_t;
};

static_assert(allocator<mimallocator>);

namespace internal::hands::off {
/// A C++ standard allocator that uses mimallocator for memory allocation.
/// This allocator is suitable for use with STL containers.
template <typename T>
class std_mimallocator {
public:
  using value_type = T;

  std_mimallocator() noexcept = default;

  template <typename U>
  std_mimallocator(const std_mimallocator<U>&) noexcept {
  }

  [[nodiscard]] auto allocate(std::size_t n) -> T* {
    auto blk = alloc_.allocate(n * sizeof(T), std::align_val_t{alignof(T)});
    if (not blk) {
      throw std::bad_alloc{};
    }
    return reinterpret_cast<T*>(blk.ptr);
  }

  auto deallocate(T* p, std::size_t n) noexcept -> void {
    alloc_.deallocate(block{reinterpret_cast<std::byte*>(p), n * sizeof(T)},
                      std::align_val_t{alignof(T)});
  }

  template <typename U>
  friend auto
  operator==(const std_mimallocator&, const std_mimallocator<U>&) noexcept
    -> bool {
    return true;
  }

  template <typename U>
  friend auto
  operator!=(const std_mimallocator&, const std_mimallocator<U>&) noexcept
    -> bool {
    return false;
  }

private:
  static inline mimallocator alloc_;
};
} // namespace internal::hands::off

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

  auto stats() const -> const stats& {
    return stats_;
  }

private:
  Inner inner_;
  struct stats stats_;
};

static_assert(allocator<stats_allocator<mimallocator>>);
static_assert(allocator_with_stats<stats_allocator<mimallocator>>);

template <allocator Inner>
class tracking_allocator {
public:
  template <typename... Ts>
  tracking_allocator(Ts&&... ts) : inner_{std::forward<Ts>(ts)...} {
    sizes_.reserve(1'000'000);
  }
  auto allocate(std::size_t size, std::align_val_t alignment) noexcept
    -> block {
    auto blk = inner_.allocate(size, alignment);
    sizes_[blk.ptr] = blk.size;
    return blk;
  }

  auto reallocate(block blk, std::size_t new_size,
                  std::align_val_t alignment) noexcept -> block {
    auto new_block = inner_.reallocate(blk, new_size, alignment);
    if (new_block.ptr != blk.ptr) {
      sizes_[blk.ptr] = 0;
    }
    sizes_[new_block.ptr] = new_block.size;
    return new_block;
  }

  auto deallocate(block blk, std::align_val_t alignment) noexcept
    -> std::size_t {
    auto size = inner_.deallocate(blk, alignment);
    auto tracked = sizes_.find(blk.ptr);
    if (tracked == sizes_.end()) {
      TENZIR_WARN("deallocation for unaccounted allocation");
      return size;
    }
    if (size != tracked->second) {
      TENZIR_WARN("mismatched dealloc request with tracked size: {} vs {}",
                  size, tracked->second);
    }
    return tracked->second;
  }

  auto stats() const -> const stats&
    requires allocator_with_stats<Inner>
  {
    return inner_.stats();
  }

private:
  tsl::robin_map<std::byte*, std::size_t, std::hash<std::byte*>,
                 std::equal_to<std::byte*>,
                 internal::hands::off::std_mimallocator<
                   std::pair<std::byte* const, std::size_t>>>
    sizes_;
  Inner inner_;
};

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

  auto stats() const -> const stats&
    requires allocator_with_stats<Inner>
  {
    return inner_.stats_();
  }

private:
  Inner& inner_;
};

using global_allocator_t = stats_allocator<mimallocator>;
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

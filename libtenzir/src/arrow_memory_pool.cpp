//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/arrow_memory_pool.hpp"

#include "tenzir/detail/assert.hpp"

#include <arrow/result.h>
#include <arrow/status.h>

#include <atomic>
#include <cstdint>
#include <mimalloc.h>

namespace tenzir {

namespace {

alignas(__STDCPP_DEFAULT_NEW_ALIGNMENT__) int64_t zero_size_area[2];
auto* const kZeroSizeArea = reinterpret_cast<uint8_t*>(&zero_size_area);

/// A custom Arrow memory pool implementation using mimalloc.
///
/// This class provides an Arrow-compatible memory pool interface backed by
/// mimalloc, which offers better performance characteristics than the default
/// system allocator for many workloads.
class mimalloc_memory_pool final : public arrow::MemoryPool {
public:
  mimalloc_memory_pool() {
    // Configure mimalloc for optimal memory management.
    // These settings balance memory reuse with returning memory to the OS.
    mi_option_set(mi_option_reset_delay, 100);
    mi_option_set(mi_option_reset_decommits, 1);
  }

  ~mimalloc_memory_pool() override = default;

  // Disable copy and move
  mimalloc_memory_pool(const mimalloc_memory_pool&) = delete;
  mimalloc_memory_pool& operator=(const mimalloc_memory_pool&) = delete;
  mimalloc_memory_pool(mimalloc_memory_pool&&) = delete;
  mimalloc_memory_pool& operator=(mimalloc_memory_pool&&) = delete;

  auto update_memory_usage_allocate(int64_t new_alloc) -> void {
    total_bytes_allocated_.fetch_add(new_alloc, std::memory_order_relaxed);
    const auto previous_current_usage
      = bytes_allocated_.fetch_add(new_alloc, std::memory_order_relaxed);
    const auto new_current_usage = previous_current_usage + new_alloc;
    int64_t old_max = max_memory_;
    while (old_max < new_current_usage
           && ! max_memory_.compare_exchange_weak(old_max, new_current_usage,
                                                  std::memory_order_relaxed)) {
    }
  }

  auto Allocate(int64_t size, int64_t alignment, uint8_t** out)
    -> arrow::Status override {
    if (size < 0) {
      return arrow::Status::Invalid("Allocation size must be non-negative");
    }
    num_allocations_.fetch_add(1, std::memory_order_relaxed);
    if (size == 0) {
      *out = kZeroSizeArea;
      return arrow::Status::OK();
    }
    void* ptr = mi_malloc_aligned(static_cast<size_t>(size),
                                  static_cast<size_t>(alignment));
    if (ptr == nullptr) {
      return arrow::Status::OutOfMemory("mimalloc allocation failed for size ",
                                        size);
    }
    *out = static_cast<uint8_t*>(ptr);
    update_memory_usage_allocate(size);
    return arrow::Status::OK();
  }

  auto Reallocate(int64_t old_size, int64_t new_size, int64_t alignment,
                  uint8_t** ptr) -> arrow::Status override {
    TENZIR_ASSERT_EXPENSIVE(*ptr != nullptr);
    if (new_size < 0) {
      return arrow::Status::Invalid("Reallocation size must be non-negative");
    }
    num_allocations_.fetch_add(1, std::memory_order_relaxed);
    if (new_size == 0) {
      Free(*ptr, old_size, alignment);
      *ptr = kZeroSizeArea;
      return arrow::Status::OK();
    }

    if (*ptr == kZeroSizeArea) {
      TENZIR_ASSERT_EXPENSIVE(old_size == 0);
      return Allocate(new_size, alignment, ptr);
    }
    void* new_ptr = mi_realloc_aligned(*ptr, static_cast<size_t>(new_size),
                                       static_cast<size_t>(alignment));
    if (new_ptr == nullptr) {
      return arrow::Status::OutOfMemory(
        "mimalloc reallocation failed for size ", new_size);
    }
    *ptr = static_cast<uint8_t*>(new_ptr);
    update_memory_usage_allocate(new_size - old_size);
    return arrow::Status::OK();
  }

  auto Free(uint8_t* ptr, int64_t size, int64_t alignment) -> void override {
    (void)alignment; // alignment is not needed for free
    TENZIR_ASSERT_EXPENSIVE(ptr != nullptr);
    if (ptr == kZeroSizeArea) {
      TENZIR_ASSERT_EXPENSIVE(size == 0);
      return;
    }
    mi_free(ptr);
    bytes_allocated_.fetch_sub(size, std::memory_order_relaxed);
  }

  auto bytes_allocated() const -> int64_t override {
    return bytes_allocated_.load(std::memory_order_relaxed);
  }

  auto total_bytes_allocated() const -> int64_t override {
    return total_bytes_allocated_.load(std::memory_order_relaxed);
  }

  auto max_memory() const -> int64_t override {
    return max_memory_;
  }

  auto num_allocations() const -> int64_t override {
    return num_allocations_;
  }

  auto backend_name() const -> std::string override {
    return "mimalloc";
  }

private:
  std::atomic<int64_t> bytes_allocated_{0};
  std::atomic<int64_t> total_bytes_allocated_{0};
  std::atomic<int64_t> max_memory_{0};
  std::atomic<int64_t> num_allocations_{0};
};

} // namespace

auto arrow_memory_pool() noexcept -> arrow::MemoryPool* {
  static mimalloc_memory_pool pool;
  return &pool;
}

} // namespace tenzir

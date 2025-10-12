//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/arrow_memory_pool.hpp"

#include <arrow/result.h>
#include <arrow/status.h>

#include <atomic>
#include <cstdint>
#include <mimalloc.h>

namespace tenzir {

namespace {

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

  auto Allocate(int64_t size, int64_t alignment, uint8_t** out)
    -> arrow::Status override {
    if (size < 0) {
      return arrow::Status::Invalid("Allocation size must be non-negative");
    }
    if (size == 0) {
      *out = nullptr;
      return arrow::Status::OK();
    }

    void* ptr = mi_malloc_aligned(static_cast<size_t>(size),
                                  static_cast<size_t>(alignment));
    if (ptr == nullptr) {
      return arrow::Status::OutOfMemory("mimalloc allocation failed for size ",
                                        size);
    }

    *out = static_cast<uint8_t*>(ptr);
    bytes_allocated_.fetch_add(size, std::memory_order_relaxed);
    return arrow::Status::OK();
  }

  auto Reallocate(int64_t old_size, int64_t new_size, int64_t alignment,
                  uint8_t** ptr) -> arrow::Status override {
    if (new_size < 0) {
      return arrow::Status::Invalid("Reallocation size must be non-negative");
    }

    if (new_size == 0) {
      if (*ptr != nullptr) {
        mi_free(*ptr);
        bytes_allocated_.fetch_sub(old_size, std::memory_order_relaxed);
        *ptr = nullptr;
      }
      return arrow::Status::OK();
    }

    if (*ptr == nullptr) {
      // Treat as regular allocation
      return Allocate(new_size, alignment, ptr);
    }

    void* new_ptr = mi_realloc_aligned(*ptr, static_cast<size_t>(new_size),
                                       static_cast<size_t>(alignment));
    if (new_ptr == nullptr) {
      return arrow::Status::OutOfMemory(
        "mimalloc reallocation failed for size ", new_size);
    }

    *ptr = static_cast<uint8_t*>(new_ptr);
    bytes_allocated_.fetch_add(new_size - old_size, std::memory_order_relaxed);
    return arrow::Status::OK();
  }

  auto Free(uint8_t* buffer, int64_t size, int64_t alignment) -> void override {
    (void)alignment; // alignment is not needed for free
    if (buffer != nullptr) {
      mi_free(buffer);
      bytes_allocated_.fetch_sub(size, std::memory_order_relaxed);
    }
  }

  auto bytes_allocated() const -> int64_t override {
    return bytes_allocated_.load(std::memory_order_relaxed);
  }

  auto total_bytes_allocated() const -> int64_t override {
    // For this implementation, total_bytes_allocated is the same as
    // bytes_allocated since we track allocations continuously without resetting
    // the counter.
    return bytes_allocated_.load(std::memory_order_relaxed);
  }

  auto max_memory() const -> int64_t override {
    // mimalloc doesn't track maximum memory usage directly.
    // Return -1 to indicate this metric is not available.
    return -1;
  }

  auto num_allocations() const -> int64_t override {
    // mimalloc doesn't track the number of active allocations directly.
    // Return -1 to indicate this metric is not available.
    return -1;
  }

  auto backend_name() const -> std::string override {
    return "mimalloc";
  }

private:
  std::atomic<int64_t> bytes_allocated_{0};
};

} // namespace

auto arrow_memory_pool() noexcept -> arrow::MemoryPool* {
  // Thread-safe static initialization (C++11 §6.7 [stmt.dcl] p4)
  static mimalloc_memory_pool pool;
  return &pool;
}

} // namespace tenzir

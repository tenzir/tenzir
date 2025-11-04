//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/arrow_memory_pool.hpp"

#include "tenzir/allocator.hpp"
#include "tenzir/detail/assert.hpp"

#include <arrow/result.h>
#include <arrow/status.h>

#include <cstdint>

namespace tenzir {

#if TENZIR_SELECT_ALLOCATOR == TENZIR_SELECT_ALLOCATOR_NONE

auto arrow_memory_pool() noexcept -> arrow::MemoryPool* {
  return arrow::default_memory_pool();
}

#else

namespace {

alignas(__STDCPP_DEFAULT_NEW_ALIGNMENT__) int64_t zero_size_area[2];
auto* const kZeroSizeArea = reinterpret_cast<uint8_t*>(&zero_size_area);

/// A custom Arrow memory pool implementation using our own allocator framework.
class memory_pool final : public arrow::MemoryPool {
public:
  memory_pool() = default;

  ~memory_pool() override = default;

  // Disable copy and move
  memory_pool(const memory_pool&) = delete;
  memory_pool& operator=(const memory_pool&) = delete;
  memory_pool(memory_pool&&) = delete;
  memory_pool& operator=(memory_pool&&) = delete;

  auto Allocate(int64_t size, int64_t alignment, uint8_t** out)
    -> arrow::Status override {
    if (size < 0) {
      return arrow::Status::Invalid("Allocation size must be non-negative");
    }
    if (size == 0) {
      *out = kZeroSizeArea;
      return arrow::Status::OK();
    }
    auto* const ptr = memory::arrow_allocator().allocate(
      size, std::align_val_t{static_cast<size_t>(alignment)});
    if (ptr == nullptr) {
      return arrow::Status::OutOfMemory("Allocation failed for size ", size);
    }
    *out = reinterpret_cast<std::uint8_t*>(ptr);
    return arrow::Status::OK();
  }

  auto Reallocate(int64_t old_size, int64_t new_size, int64_t alignment,
                  uint8_t** ptr) -> arrow::Status override {
    TENZIR_ASSERT_EXPENSIVE(*ptr != nullptr);
    if (new_size < 0) {
      return arrow::Status::Invalid("Reallocation size must be non-negative");
    }
    if (new_size == 0) {
      Free(*ptr, old_size, alignment);
      *ptr = kZeroSizeArea;
      return arrow::Status::OK();
    }
    if (*ptr == kZeroSizeArea) {
      TENZIR_ASSERT_EXPENSIVE(old_size == 0);
      return Allocate(new_size, alignment, ptr);
    }
    auto* const new_ptr = memory::arrow_allocator().reallocate(
      *ptr, new_size, std::align_val_t{static_cast<size_t>(alignment)});
    if (new_ptr == nullptr) {
      return arrow::Status::OutOfMemory("Reallocation failed for size ",
                                        new_size);
    }
    *ptr = reinterpret_cast<std::uint8_t*>(new_ptr);
    return arrow::Status::OK();
  }

  auto Free(uint8_t* ptr, int64_t size, int64_t alignment) -> void override {
    (void)alignment; // alignment is not needed for free
    TENZIR_ASSERT_EXPENSIVE(ptr != nullptr);
    if (ptr == kZeroSizeArea) {
      TENZIR_ASSERT_EXPENSIVE(size == 0);
      return;
    }
    memory::arrow_allocator().deallocate(ptr);
  }

  auto bytes_allocated() const -> int64_t override {
    return memory::arrow_allocator().stats().bytes_current;
  }

  auto total_bytes_allocated() const -> int64_t override {
    return memory::arrow_allocator().stats().bytes_cumulative;
  }

  auto max_memory() const -> int64_t override {
    return memory::arrow_allocator().stats().bytes_peak;
  }

  auto num_allocations() const -> int64_t override {
    return memory::arrow_allocator().stats().num_calls;
  }

  auto backend_name() const -> std::string override {
    return std::string{memory::arrow_allocator().backend_name()};
  }
};

} // namespace

auto arrow_memory_pool() noexcept -> arrow::MemoryPool* {
  constinit static auto pool = memory_pool{};
  return &pool;
}

#endif

} // namespace tenzir

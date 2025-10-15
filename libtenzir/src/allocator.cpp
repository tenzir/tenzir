//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/allocator.hpp"

#include "tenzir/logger.hpp"

#include <mimalloc.h>

namespace tenzir::memory {

auto stats::update_max_bytes(std::int64_t new_usage) noexcept -> void {
  auto old_max = bytes_max.load();
  while (old_max < new_usage
         && ! bytes_max.compare_exchange_weak(old_max, new_usage)) {
  }
}

auto stats::add_allocation() noexcept -> void {
  auto new_count = allocations_current.fetch_add(1) + 1;
  auto old_max = allocations_max.load();
  while (old_max < new_count
         && ! allocations_max.compare_exchange_weak(old_max, new_count)) {
  }
}

auto stats::note_allocation(std::int64_t add) noexcept -> void {
  bytes_total.fetch_add(add);
  num_calls.fetch_add(1);
  add_allocation();
  const auto previous_current_usage = bytes_current.fetch_add(add);
  const auto new_current_usage = previous_current_usage + add;
  update_max_bytes(new_current_usage);
}

auto stats::note_reallocation(const reallocation_result& r) noexcept -> void {
  num_calls.fetch_add(1);
  if (r.new_block.ptr == nullptr) {
    note_deallocation(r.true_old_block.size);
    return;
  }
  if (r.new_block.ptr != r.true_old_block.ptr) {
    note_deallocation(r.true_old_block.size);
    note_allocation(r.new_block.size);
    return;
  } else {
    const auto diff = static_cast<std::int64_t>(r.new_block.size)
                      - static_cast<std::int64_t>(r.true_old_block.size);
    const auto previous_current_usage = bytes_current.fetch_add(diff);
    if (diff > 0) {
      const auto new_current_usage = previous_current_usage + diff;
      update_max_bytes(new_current_usage);
    }
  }
}

auto stats::note_deallocation(std::int64_t remove) noexcept -> void {
  allocations_current.fetch_sub(1);
  bytes_current.fetch_sub(remove);
}

mimallocator::mimallocator() noexcept {
  // Configure mimalloc for optimal memory management.
  // These settings balance memory reuse with returning memory to the OS.
  mi_option_set(mi_option_reset_delay, 100);
  mi_option_set(mi_option_reset_decommits, 1);
}

auto mimallocator::allocate(std::size_t size,
                            std::align_val_t alignment) noexcept -> block {
  auto* ptr = static_cast<std::byte*>(
    mi_malloc_aligned(size, std::to_underlying(alignment)));
  size = mi_usable_size(ptr);
  if (ptr == nullptr) {
    return {};
  }
  return block{
    ptr,
    size,
  };
}

auto mimallocator::reallocate(block old_block, std::size_t new_size,
                              std::align_val_t alignment) noexcept
  -> reallocation_result {
  old_block.size = mi_usable_size(old_block.ptr);
  if (old_block.size == new_size) {
    return {
      .true_old_block = old_block,
      .new_block = old_block,
    };
  }
  if (new_size == 0) {
    deallocate(old_block, alignment);
    return {
      old_block,
      block{},
    };
  }
  old_block.size = mi_good_size(old_block.size);
  new_size = mi_good_size(new_size);
  void* new_ptr = mi_realloc_aligned(old_block.ptr, new_size,
                                     std::to_underlying(alignment));
  new_size = mi_usable_size(new_ptr);
  auto new_block = block{
    static_cast<std::byte*>(new_ptr),
    new_size,
  };
  if (new_ptr == nullptr) {
    return {
      old_block,
      block{},
    };
  }
  return {
    .true_old_block = old_block,
    .new_block = new_block,
  };
}

auto mimallocator::deallocate(block blk, std::align_val_t alignment) noexcept
  -> std::size_t {
  (void)alignment;
  if (blk.ptr == nullptr) {
    TENZIR_ASSERT_EXPENSIVE(blk.size == 0);
    return 0;
  }
  blk.size = mi_usable_size(blk.ptr);
  mi_free(blk.ptr);
  return blk.size;
}

auto global_allocator() noexcept -> global_allocator_t& {
  static auto alloc = global_allocator_t{};
  return alloc;
}

auto arrow_allocator() noexcept -> separated_allocator_t& {
  static auto instance = separated_allocator_t{global_allocator()};
  return instance;
}

auto cpp_allocator() noexcept -> separated_allocator_t& {
  static auto instance = separated_allocator_t{global_allocator()};
  return instance;
}

} // namespace tenzir::memory

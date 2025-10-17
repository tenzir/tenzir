//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/allocator.hpp"

#include "tenzir/si_literals.hpp"

#include <caf/actor_system.hpp>

#include <cstdio>

#if TENZIR_LINUX
#  include <malloc.h>
#elif TENZIR_MACOS
#  include <malloc/malloc.h>
#  define malloc_usable_size malloc_size
#endif

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

namespace mimalloc {

namespace {

struct init {
  init() {
    // Configure mimalloc for optimal memory management.
    // These settings balance memory reuse with returning memory to the OS.
    mi_option_set(mi_option_reset_delay, 100);
    mi_option_set(mi_option_reset_decommits, 1);
  }
} init_;

} // namespace

auto allocate(std::size_t size, std::align_val_t alignment) noexcept -> block {
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

auto deallocate(block blk, std::align_val_t alignment) noexcept -> std::size_t {
  (void)alignment;
  if (blk.ptr == nullptr) {
    TENZIR_ASSERT_EXPENSIVE(blk.size == 0);
    return 0;
  }
  blk.size = mi_usable_size(blk.ptr);
  mi_free(blk.ptr);
  return blk.size;
}

auto reallocate(block old_block, std::size_t new_size,
                std::align_val_t alignment) noexcept -> reallocation_result {
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

auto trim() noexcept -> void {
  mi_collect(false);
}

auto typed_allocator::allocate(std::size_t size,
                               std::align_val_t alignment) noexcept -> block {
  return mimalloc::allocate(size, alignment);
}

auto typed_allocator::reallocate(block old_block, std::size_t new_size,
                                 std::align_val_t alignment) noexcept
  -> reallocation_result {
  return mimalloc::reallocate(old_block, new_size, alignment);
}

auto typed_allocator::deallocate(block old_block,
                                 std::align_val_t alignment) noexcept
  -> std::size_t {
  return mimalloc::deallocate(old_block, alignment);
}

auto typed_allocator::trim() noexcept -> void {
  return mimalloc::trim();
}

auto typed_allocator::backend() noexcept -> std::string_view {
  return "mimalloc";
}

auto erased_allocator() noexcept -> erased_allocator {
  return {
    .allocate = allocate,
    .reallocate = reallocate,
    .deallocate = deallocate,
    .trim = trim,
    .backend_ = "mimalloc",
  };
}

} // namespace mimalloc

namespace system {

auto round_to_alignment(std::size_t size, std::align_val_t alignment) {
  const auto alignment_s = std::to_underlying(alignment);
  return ((size + alignment_s - 1) / alignment_s) * alignment_s;
}

auto allocate(std::size_t size, std::align_val_t alignment) noexcept -> block {
  size = round_to_alignment(size, alignment);
  auto* ptr = static_cast<std::byte*>(
    ::aligned_alloc(std::to_underlying(alignment), size));
  if (ptr == nullptr) {
    std::fprintf(stderr, "null alloc for size %zu, %zu\n", size,
                 std::to_underlying(alignment));
    return {};
  }
  size = ::malloc_usable_size(ptr);
  return block{
    ptr,
    size,
  };
}

auto deallocate(block blk, std::align_val_t alignment) noexcept -> std::size_t {
  (void)alignment;
  blk.size = ::malloc_usable_size(blk.ptr);
  ::free(blk.ptr);
  return blk.size;
}

auto reallocate(block old_block, std::size_t new_size,
                std::align_val_t alignment) noexcept -> reallocation_result {
  old_block.size = ::malloc_usable_size(old_block.ptr);
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
  new_size = round_to_alignment(new_size, alignment);
  void* new_ptr = ::realloc(old_block.ptr, new_size);
  new_size = ::malloc_usable_size(new_ptr);
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

auto trim() noexcept -> void {
#if (defined(__GLIBC__))
  using namespace si_literals;
  constexpr auto padding = 512_Mi;
  ::malloc_trim(padding);
#endif
}

auto typed_allocator::allocate(std::size_t size,
                               std::align_val_t alignment) noexcept -> block {
  return system::allocate(size, alignment);
}

auto typed_allocator::reallocate(block old_block, std::size_t new_size,
                                 std::align_val_t alignment) noexcept
  -> reallocation_result {
  return system::reallocate(old_block, new_size, alignment);
}

auto typed_allocator::deallocate(block old_block,
                                 std::align_val_t alignment) noexcept
  -> std::size_t {
  return system::deallocate(old_block, alignment);
}

auto typed_allocator::trim() noexcept -> void {
  return system::trim();
}

auto typed_allocator::backend() noexcept -> std::string_view {
  return "mimalloc";
}

auto erased_allocator() noexcept -> erased_allocator {
  return {
    .allocate = allocate,
    .reallocate = reallocate,
    .deallocate = deallocate,
    .trim = trim,
    .backend_ = "system",
  };
}

} // namespace system

auto selected_alloc() -> erased_allocator {
  const auto env = ::getenv("TENZIR_ALLOCATOR");
  if (not env) {
    return mimalloc::erased_allocator();
  }
  const auto env_str = std::string_view{env};
  if (env_str.empty() or env_str == "mimalloc") {
    return mimalloc::erased_allocator();
  }
  if (env_str == "system") {
    return system::erased_allocator();
  }
  std::fprintf(stderr,
               "FATAL ERROR: unknown TENZIR_ALLOCATOR: '%s'\n"
               "known values are 'mimalloc' and 'system'\n",
               env_str.data());
  std::exit(EXIT_FAILURE);
  std::unreachable();
}

auto global_allocator() noexcept -> global_allocator_t& {
  static auto alloc = global_allocator_t{selected_alloc()};
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

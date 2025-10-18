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

namespace {

constexpr auto align_mask(std::align_val_t alignment) noexcept
  -> std::uintptr_t {
  return std::to_underlying(alignment) - 1;
}

constexpr auto is_aligned(void* ptr, std::align_val_t alignment) noexcept
  -> bool {
  return (reinterpret_cast<std::uintptr_t>(ptr) & align_mask(alignment)) == 0;
}

constexpr auto is_aligned(std::size_t size, std::align_val_t alignment) noexcept
  -> bool {
  return (size & align_mask(alignment)) == 0;
}

constexpr auto align(std::size_t size, std::align_val_t alignment) noexcept
  -> std::size_t {
  const auto remainder = size & align_mask(alignment);
  return size + (remainder > 0) * (std::to_underlying(alignment) - remainder);
}

template <auto alloc_impl, auto aligned_alloc_impl,
          std::align_val_t default_alignment, auto size_impl, auto realloc_impl,
          auto aligned_realloc_impl, auto free_impl>
struct allocator_impl {
  [[gnu::hot]] static auto
  allocate(std::size_t size, std::align_val_t alignment) noexcept -> block {
    auto* const ptr = alignment <= default_alignment
                        ? alloc_impl(size)
                        : aligned_alloc_impl(align(size, alignment),
                                             std::to_underlying(alignment));
    if (ptr == nullptr) {
      return {};
    }
    // size = size_impl(ptr);
    return block{
      static_cast<std::byte*>(ptr),
      size,
    };
  }

  [[gnu::hot]] static auto
  deallocate(block blk, std::align_val_t alignment) noexcept -> std::size_t {
    (void)alignment;
    if (blk.ptr == nullptr) {
      TENZIR_ASSERT_EXPENSIVE(blk.size == 0);
      return 0;
    }
    // blk.size = size_impl(blk.ptr);
    free_impl(blk.ptr);
    return blk.size;
  }

  [[gnu::hot]] static auto reallocate(block old_block, std::size_t new_size,
                                      std::align_val_t alignment) noexcept
    -> reallocation_result {
    // old_block.size = size_impl(old_block.ptr);
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
    void* new_ptr
      = alignment <= default_alignment
          ? realloc_impl(old_block.ptr, new_size)
          : aligned_realloc_impl(old_block.ptr, align(new_size, alignment),
                                 std::to_underlying(alignment));
    // new_size = size_impl(new_ptr);
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
};

} // namespace

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

using allocator
  = allocator_impl<mi_malloc, mi_malloc_aligned, std::align_val_t{16},
                   mi_usable_size, mi_realloc, mi_realloc_aligned, mi_free>;

auto trim() noexcept -> void {
  mi_collect(false);
}

constexpr auto erased_allocator() noexcept -> erased_allocator {
  return {
    .allocate = allocator::allocate,
    .reallocate = allocator::reallocate,
    .deallocate = allocator::deallocate,
    .trim = trim,
    .backend_ = "mimalloc",
  };
}

} // namespace mimalloc

namespace system {

namespace {

/// * Change to typed allocators/virtual functions. Declare the typed allocators
/// in the header and mark them final?
/// * Refactor stats gathering into the allocators themselves, allowing us to
///  * only call `size_impl` conditionally ->
///  * change the alloc signature to only deal in pointers (not blocks)
///  * alloc_align/alloc_size attributes

[[gnu::alloc_size(1), gnu::alloc_align(2)]]
auto malloc_aligned(std::size_t size, std::size_t alignment) noexcept -> void* {
  return std::aligned_alloc(alignment, size);
}

[[gnu::alloc_size(2), gnu::alloc_align(3)]]
auto realloc_aligned(void* ptr, std::size_t new_size,
                     std::size_t alignment) noexcept -> void* {
  auto* new_ptr_happy = std::realloc(ptr, new_size);
  if (is_aligned(new_ptr_happy, std::align_val_t{alignment})) {
    return new_ptr_happy;
  }
  if (not new_ptr_happy) {
    new_ptr_happy = ptr;
  }
  auto size = malloc_usable_size(new_ptr_happy);
  auto* new_ptr_expensive = malloc_aligned(new_size, alignment);
  std::memcpy(new_ptr_expensive, new_ptr_happy, size);
  free(new_ptr_happy);
  return new_ptr_expensive;
}
} // namespace

using allocator
  = allocator_impl<malloc, malloc_aligned, std::align_val_t{16},
                   malloc_usable_size, realloc, realloc_aligned, free>;

auto trim() noexcept -> void {
#if (defined(__GLIBC__))
  using namespace si_literals;
  constexpr auto padding = 512_Mi;
  ::malloc_trim(padding);
#endif
}

constexpr auto erased_allocator() noexcept -> erased_allocator {
  return {
    .allocate = allocator::allocate,
    .reallocate = allocator::reallocate,
    .deallocate = allocator::deallocate,
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

[[nodiscard, gnu::hot, gnu::const]] auto global_allocator() noexcept
  -> global_allocator_t& {
  static auto alloc = global_allocator_t{selected_alloc()};
  return alloc;
}

[[nodiscard, gnu::hot, gnu::const]] auto arrow_allocator() noexcept
  -> separated_allocator_t& {
  static auto instance = global_allocator_t{selected_alloc()};
  return instance;
}

[[nodiscard, gnu::hot, gnu::const]] auto cpp_allocator() noexcept
  -> separated_allocator_t& {
  static auto instance = global_allocator_t{selected_alloc()};
  return instance;
}

} // namespace tenzir::memory

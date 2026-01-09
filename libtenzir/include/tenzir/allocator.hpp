//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include "tenzir/allocator_config.hpp"
#include "tenzir/detail/assert.hpp"

#include <caf/abstract_actor.hpp>
#include <caf/logger.hpp>
#include <tsl/robin_map.h>

#include <atomic>
#include <concepts>
#include <cstddef>
#include <mutex>
#include <shared_mutex>
#if TENZIR_ALLOCATOR_HAS_JEMALLOC
#  include <jemalloc/jemalloc.h>
#endif
#if TENZIR_ALLOCATOR_HAS_MIMALLOC
#  include <mimalloc.h>
#endif
#include <new>
#include <string_view>

namespace tenzir::memory {

enum class backend {
  system,
  jemalloc,
  mimalloc,
};

struct stats {
  alignas(std::hardware_destructive_interference_size)
    std::atomic<std::int64_t> bytes_current;
  alignas(std::hardware_destructive_interference_size)
    std::atomic<std::int64_t> bytes_cumulative;
  alignas(std::hardware_destructive_interference_size)
    std::atomic<std::int64_t> bytes_peak;
  alignas(std::hardware_destructive_interference_size)
    std::atomic<std::int64_t> num_calls;
  alignas(std::hardware_destructive_interference_size)
    std::atomic<std::int64_t> allocations_current;
  alignas(std::hardware_destructive_interference_size)
    std::atomic<std::int64_t> allocations_cumulative;
  alignas(std::hardware_destructive_interference_size)
    std::atomic<std::int64_t> allocations_peak;

  auto note_allocation(std::int64_t add) noexcept -> void;
  auto note_reallocation(bool new_location, std::int64_t old_size,
                         std::int64_t new_size) noexcept -> void;
  auto note_deallocation(std::int64_t remove) noexcept -> void;
  auto update_max_bytes(std::int64_t new_usage) noexcept -> void;
};

namespace detail {

constexpr auto align_mask(std::align_val_t alignment) noexcept
  -> std::uintptr_t {
  return std::to_underlying(alignment) - 1;
}

constexpr auto mod(std::size_t N, std::align_val_t alignment) noexcept
  -> std::size_t {
  return N & align_mask(alignment);
}

constexpr auto
round_to_alignment(std::size_t N, std::align_val_t alignment) noexcept
  -> std::size_t {
  const auto m = mod(N, alignment);
  return N - m + std::to_underlying(alignment);
}

class actor_name {
public:
  operator std::string_view() const noexcept {
    /// If the last character is a null, the name may be shorter.
    if (storage_.back() == '\0') {
      return {storage_.data()};
    }
    /// Otherwise, all characters are part of the string.
    return {storage_.data(), storage_.size()};
  }

  friend auto operator<=>(const actor_name& lhs, const actor_name& rhs) noexcept
    -> std::strong_ordering
    = default;

  friend auto operator==(const actor_name&, const actor_name&) noexcept -> bool
    = default;

  [[nodiscard]] static auto current() noexcept -> actor_name {
    actor_name res;
    res.make_current();
    return res;
  }

  auto make_current() noexcept -> void {
    const auto aptr = caf::logger::thread_local_aptr();
    if (not aptr) {
      return;
    }
    const char* const name = aptr->name();
#ifndef __clang__
#  pragma GCC diagnostic push
#  pragma GCC diagnostic ignored "-Wstringop-truncation"
#endif
    std::strncpy(storage_.data(), name, storage_.size());
#ifndef __clang__
#  pragma GCC diagnostic pop
#endif
  }

private:
  std::array<char, 16> storage_ = {}; // make sure this is 0 initialized.
};

static_assert(sizeof(actor_name) == 16);
static_assert(alignof(actor_name) <= 8);

/// A tag placed at the beginning of an allocation
// [padding][tag][data...]
// ^storage_ptr
//               ^data_ptr
// This relies on tag always being right up against the data section. This is
// required to allow us to get from the data pointer to the tag pointer. The tag
// then contains the allocations alignment, which we need to get back to the
// storage pointer.
struct allocation_tag {
  class actor_name actor_name;
  std::align_val_t alignment;

  struct tag_and_data {
    allocation_tag* tag_ptr;
    void* data_ptr;
  };

  static auto
  storage_size_for(std::size_t data_size, std::align_val_t alignment) noexcept
    -> std::size_t {
    alignment = std::align_val_t{
      std::max(std::to_underlying(alignment), alignof(allocation_tag))};
    data_size
      = round_to_alignment(data_size + sizeof(allocation_tag), alignment);
    return data_size;
  }

  static auto obtain_from(void* storage_ptr,
                          std::align_val_t alignment) noexcept -> tag_and_data {
    alignment = std::align_val_t{
      std::max(std::to_underlying(alignment), alignof(allocation_tag))};
    constexpr auto tag_size = sizeof(allocation_tag);
    auto* const data_ptr = reinterpret_cast<void*>(round_to_alignment(
      reinterpret_cast<std::uintptr_t>(storage_ptr) + tag_size, alignment));
    allocation_tag* const tag_ptr
      = reinterpret_cast<allocation_tag*>(data_ptr) - 1;
    return {
      .tag_ptr = tag_ptr,
      .data_ptr = data_ptr,
    };
  }

  static auto create_at(void* storage_ptr, std::align_val_t alignment) noexcept
    -> tag_and_data {
    auto res = obtain_from(storage_ptr, alignment);
    // Create tag with alignment information
    res.tag_ptr = std::construct_at(res.tag_ptr);
    res.tag_ptr->alignment = alignment;
    res.tag_ptr->actor_name.make_current();
    return res;
  }

  struct storage_and_tag {
    void* storage_ptr;
    const allocation_tag& tag;
  };

  /// Gets both the storage pointer and tag from a data pointer
  [[nodiscard]] static auto get_storage_and_tag(void* data_ptr) noexcept
    -> storage_and_tag {
    auto* tag_ptr = static_cast<allocation_tag*>(data_ptr) - 1;
    const auto prefix_size
      = round_to_alignment(sizeof(allocation_tag), tag_ptr->alignment);
    void* const storage_ptr
      = reinterpret_cast<std::byte*>(data_ptr) - prefix_size;
    return {
      .storage_ptr = storage_ptr,
      .tag = *tag_ptr,
    };
  }
};

struct actor_stat {
  /// Total bytes this actor has allocated
  alignas(std::hardware_destructive_interference_size)
    std::atomic<std::int64_t> bytes_allocated_cumulative
    = 0;
  /// Total bytes this actor has deallocated
  alignas(std::hardware_destructive_interference_size)
    std::atomic<std::int64_t> bytes_deallocated_cumulative
    = 0;
  /// Total bytes this actor has reallocated (may be negative)
  alignas(std::hardware_destructive_interference_size)
    std::atomic<std::int64_t> bytes_reallocated_cumulative
    = 0;
  /// Bytes allocated by this actor that have not been deallocated
  alignas(std::hardware_destructive_interference_size)
    std::atomic<std::int64_t> bytes_alive
    = 0;
  /// Last time this actor performed any allocation/deallocation/reallocation
  alignas(std::hardware_destructive_interference_size)
    std::chrono::steady_clock::time_point last_seen
    = {};
};

struct actor_name_equal {
  static auto operator()(const actor_name& lhs, const actor_name& rhs) noexcept
    -> bool {
    return std::bit_cast<__uint128_t>(lhs) == std::bit_cast<__uint128_t>(rhs);
  }
};

struct actor_name_hash {
  static auto operator()(const actor_name& name) noexcept -> std::size_t {
    return std::hash<__uint128_t>{}(std::bit_cast<__uint128_t>(name));
  }
};

using actor_stat_allocator
  = std::pmr::polymorphic_allocator<std::pair<const actor_name, actor_stat>>;

using actor_stat_map
  = std::unordered_map<actor_name, actor_stat, actor_name_hash,
                       actor_name_equal, actor_stat_allocator>;

class actor_stats_read {
public:
  actor_stats_read() = default;
  actor_stats_read(std::shared_mutex& mut, const detail::actor_stat_map& map)
    : lock_{mut}, map_{&map} {
  }
  auto has_value() const noexcept -> bool {
    return lock_.owns_lock();
  }

  operator bool() const noexcept {
    return has_value();
  }

  auto operator*() const -> const detail::actor_stat_map& {
    return *map_;
  }

private:
  std::shared_lock<std::shared_mutex> lock_;
  const detail::actor_stat_map* map_;
};

template <typename T>
concept allocator = requires(T t, std::size_t size, std::align_val_t alignment,
                             void* ptr, const void* cptr) {
  { t.allocate(size) } noexcept -> std::same_as<void*>;
  { t.allocate(size, alignment) } noexcept -> std::same_as<void*>;
  { t.calloc(size, size) } noexcept -> std::same_as<void*>;
  { t.calloc(size, size, alignment) } noexcept -> std::same_as<void*>;
  { t.reallocate(ptr, size) } noexcept -> std::same_as<void*>;
  { t.reallocate(ptr, size, alignment) } noexcept -> std::same_as<void*>;
  { t.deallocate(ptr) } noexcept -> std::same_as<void>;
  { t.trim() } noexcept;
  { t.stats() } noexcept -> std::same_as<const stats&>;
  { t.actor_stats() } noexcept -> std::same_as<actor_stats_read>;
  { t.backend() } noexcept -> std::same_as<enum backend>;
  { t.backend_name() } noexcept -> std::same_as<std::string_view>;
  { t.size(cptr) } noexcept -> std::same_as<std::size_t>;
};

/// Concept for allocator traits types.
template <typename T>
concept allocator_trait
  = requires(std::size_t size, std::size_t alignment, std::size_t count,
             void* ptr, const void* cptr) {
      { T::default_alignment } -> std::same_as<const std::align_val_t&>;
      { T::backend_value } -> std::same_as<const backend&>;
      { T::name } -> std::convertible_to<std::string_view>;
      { T::malloc(size) } -> std::same_as<void*>;
      { T::malloc_aligned(size, alignment) } -> std::same_as<void*>;
      { T::calloc(count, size) } -> std::same_as<void*>;
      { T::calloc_aligned(count, size, alignment) } -> std::same_as<void*>;
      { T::realloc(ptr, size) } -> std::same_as<void*>;
      { T::realloc_aligned(ptr, size, alignment) } -> std::same_as<void*>;
      { T::free(ptr) } -> std::same_as<void>;
      { T::usable_size(cptr) } -> std::same_as<std::size_t>;
      { T::trim() } -> std::same_as<void>;
    };

/// A C++ allocator that directly uses the allocator traits for malloc/free.
template <allocator_trait Traits>
class basic_pmr_resource final : public std::pmr::memory_resource {
public:
  [[nodiscard]] virtual auto
  do_allocate(std::size_t bytes, std::size_t alignment) -> void* override {
    if (std::align_val_t{alignment} <= Traits::default_alignment) {
      return Traits::malloc(bytes);
    }
    return Traits::malloc_aligned(bytes, alignment);
  }

  virtual auto do_deallocate(void* ptr, std::size_t bytes,
                             std::size_t alignment) noexcept -> void override {
    TENZIR_UNUSED(bytes, alignment);
    Traits::free(ptr);
  }

  [[nodiscard]]
  virtual auto
  do_is_equal(const std::pmr::memory_resource& other) const noexcept
    -> bool override {
    return typeid(*this) == typeid(other);
  }
};

/// Simple per-actor tracking of memory. There is a map protected by a mutex,
/// which contains atomics.
/// When making any change to the data, there is two phases:
///
/// * First we obtain a shared lock and try and see if the key is in the map
///   already.
///   If it is, we can safely perform an atomic modification to it and are
///   done.
/// * Otherwise, we obtain a unique lock and try to insert the key. Notably it
///   is possible for the key to actually exist by now, since somebody else may
///   have gotten the write lock before us and inserted it.
///   Because of this, we then perform atomic modifications to the value.
template <detail::allocator_trait InternalTrait>
class actor_stats final {
public:
  auto note_allocation(const detail::allocation_tag& tag, std::int64_t size)
    -> void {
    const auto now = std::chrono::steady_clock::now();
    auto read_lock = std::shared_lock{mut_};
    auto it = allocations_by_actor_.find(tag.actor_name);
    if (it == allocations_by_actor_.end()) {
      read_lock.unlock();
      const auto write_lock = std::scoped_lock{mut_};
      const auto [it, _] = allocations_by_actor_.try_emplace(tag.actor_name);
      auto& value = it->second;
      value.bytes_alive.fetch_add(size, std::memory_order_relaxed);
      value.bytes_allocated_cumulative.fetch_add(size,
                                                 std::memory_order_relaxed);
      value.last_seen = now;
      return;
    }
    auto& value = it->second;
    value.bytes_allocated_cumulative.fetch_add(size, std::memory_order_relaxed);
    value.bytes_alive.fetch_add(size, std::memory_order_relaxed);
    if (value.last_seen < now) {
      value.last_seen = now;
    }
  }

  auto note_reallocation(const detail::allocation_tag& tag,
                         std::int64_t old_size, std::int64_t new_size) -> void {
    const auto my_name = actor_name::current();
    const auto diff = new_size - old_size;
    const auto now = std::chrono::steady_clock::now();
    auto read_lock = std::shared_lock{mut_};
    {
      const auto it = allocations_by_actor_.find(tag.actor_name);
      TENZIR_ASSERT(it != allocations_by_actor_.end());
      auto& value = it->second;
      value.bytes_alive.fetch_add(diff, std::memory_order_relaxed);
      if (value.last_seen < now) {
        value.last_seen = now;
      }
    }
    const auto it = allocations_by_actor_.find(my_name);
    if (it == allocations_by_actor_.end()) {
      read_lock.unlock();
      const auto write_lock = std::scoped_lock{mut_};
      const auto [it, _] = allocations_by_actor_.try_emplace(my_name);
      auto& value = it->second;
      value.bytes_reallocated_cumulative.fetch_add(diff,
                                                   std::memory_order_relaxed);
      value.last_seen = now;
      return;
    }
    auto& value = it->second;
    value.bytes_reallocated_cumulative.fetch_add(diff,
                                                 std::memory_order_relaxed);
    if (value.last_seen < now) {
      value.last_seen = now;
    }
  }

  auto note_deallocation(const detail::allocation_tag& tag, std::int64_t size)
    -> void {
    const auto my_name = actor_name::current();
    const auto now = std::chrono::steady_clock::now();
    auto read_lock = std::shared_lock{mut_};
    {
      const auto it = allocations_by_actor_.find(tag.actor_name);
      TENZIR_ASSERT(it != allocations_by_actor_.end());
      auto& value = it->second;
      value.bytes_alive.fetch_sub(size, std::memory_order_relaxed);
    }
    const auto it = allocations_by_actor_.find(my_name);
    if (it == allocations_by_actor_.end()) {
      read_lock.unlock();
      const auto write_lock = std::scoped_lock{mut_};
      const auto [it, _] = allocations_by_actor_.try_emplace(my_name);
      auto& value = it->second;
      value.bytes_deallocated_cumulative.fetch_add(size,
                                                   std::memory_order_relaxed);
      value.last_seen = now;
      return;
    }
    auto& value = it->second;
    value.bytes_deallocated_cumulative.fetch_add(size,
                                                 std::memory_order_relaxed);
    if (value.last_seen < now) {
      value.last_seen = now;
    }
  }

  /// Obtains a reading lock on the internal data structure and a reference to it
  [[nodiscard]] auto read() -> detail::actor_stats_read {
    return {mut_, allocations_by_actor_};
  }

  auto prune() {
    constexpr static auto prune_interval = std::chrono::minutes{10};
    const auto now = std::chrono::steady_clock::now();
    if (now - last_prune_ < prune_interval) {
      return;
    }
    const auto l = std::scoped_lock{mut_};
    last_prune_ = now;
    const auto cutoff = now - prune_interval;
    for (auto it = allocations_by_actor_.begin();
         it != allocations_by_actor_.end();) {
      auto& value = it->second;
      if (value.bytes_alive == 0 and value.last_seen < cutoff) {
        it = allocations_by_actor_.erase(it);
      } else {
        ++it;
      }
    }
  }

private:
  detail::basic_pmr_resource<InternalTrait> resource_;
  std::shared_mutex mut_;
  detail::actor_stat_map allocations_by_actor_{
    detail::actor_stat_allocator{&resource_}};
  std::chrono::steady_clock::time_point last_prune_;
};

} // namespace detail

// Polymorphic base class for the runtime switchable allocator.
struct polymorphic_allocator {
  [[nodiscard, gnu::hot, gnu::alloc_size(2)]]
  virtual auto allocate(std::size_t) noexcept -> void* = 0;
  [[nodiscard, gnu::hot, gnu::alloc_size(2), gnu::alloc_align(3)]]
  virtual auto allocate(std::size_t, std::align_val_t) noexcept -> void* = 0;
  [[nodiscard, gnu::hot, gnu::alloc_size(2, 3)]]
  virtual auto calloc(std::size_t, std::size_t) noexcept -> void* = 0;
  [[nodiscard, gnu::hot, gnu::alloc_size(2, 3), gnu::alloc_align(4)]]
  virtual auto calloc(std::size_t, std::size_t, std::align_val_t) noexcept
    -> void* = 0;
  [[nodiscard, gnu::hot, gnu::alloc_size(3)]]
  virtual auto reallocate(void*, std::size_t) noexcept -> void* = 0;
  [[nodiscard, gnu::hot, gnu::alloc_size(3), gnu::alloc_align(4)]]
  virtual auto reallocate(void*, std::size_t, std::align_val_t) noexcept
    -> void* = 0;
  virtual auto deallocate(void*) noexcept -> void = 0;
  virtual auto size(const void*) const noexcept -> std::size_t = 0;
  virtual auto trim() noexcept -> void = 0;
  virtual auto stats() const noexcept -> const struct stats& = 0;
  virtual auto actor_stats() const noexcept -> detail::actor_stats_read = 0;
  virtual auto backend() const noexcept -> enum backend = 0;
  virtual auto backend_name() const noexcept -> std::string_view = 0;
};

static_assert(detail::allocator<polymorphic_allocator>);

namespace detail {

/// We refer to this object when calling allocator.stats() on an allocator that
/// does not collect stats.
inline constexpr auto zero_stats = stats{};

alignas(__STDCPP_DEFAULT_NEW_ALIGNMENT__) inline std::byte
  zero_size_area[__STDCPP_DEFAULT_NEW_ALIGNMENT__];

/// Template allocator implementation that delegates to a traits type.
template <allocator_trait Traits, allocator_trait InternalTrait>
class basic_allocator final : public polymorphic_allocator {
public:
  constexpr explicit basic_allocator(
    struct stats* stats, detail::actor_stats<InternalTrait>* actor_stats)
    : stats_{stats}, actor_stats_{actor_stats} {
  }

  static_assert(std::align_val_t{alignof(allocation_tag)}
                <= Traits::default_alignment);

  [[nodiscard, gnu::hot, gnu::always_inline, gnu::malloc, gnu::alloc_size(2)]]
  auto allocate(std::size_t size) noexcept -> void* final override {
    if (size == 0) {
      return &zero_size_area;
    }
    if (actor_stats_) {
      size = allocation_tag::storage_size_for(size, Traits::default_alignment);
    }
    auto* const ptr = Traits::malloc(size);
    if (ptr == nullptr) {
      return nullptr;
    }
    if (stats_ || actor_stats_) {
      const auto usable = Traits::usable_size(ptr);
      if (stats_) {
        stats_->note_allocation(usable);
      }
      if (actor_stats_) {
        const auto& [tag, data_ptr]
          = allocation_tag::create_at(ptr, Traits::default_alignment);
        actor_stats_->note_allocation(*tag, usable);
        return data_ptr;
      }
    }
    return ptr;
  }

  [[nodiscard, gnu::hot, gnu::always_inline, gnu::malloc, gnu::alloc_size(2),
    gnu::alloc_align(3)]]
  auto allocate(std::size_t size, std::align_val_t alignment) noexcept
    -> void* final override {
    if (alignment <= Traits::default_alignment) {
      return allocate(size);
    }
    if (size == 0) {
      return &zero_size_area;
    }
    if (actor_stats_) {
      size = allocation_tag::storage_size_for(size, alignment);
    }
    auto* const ptr
      = Traits::malloc_aligned(size, std::to_underlying(alignment));
    if (ptr == nullptr) {
      return nullptr;
    }
    if (stats_ || actor_stats_) {
      const auto usable = Traits::usable_size(ptr);
      if (stats_) {
        stats_->note_allocation(usable);
      }
      if (actor_stats_) {
        const auto& [tag, data_ptr] = allocation_tag::create_at(ptr, alignment);
        actor_stats_->note_allocation(*tag, usable);
        return data_ptr;
      }
    }
    return ptr;
  }

  [[nodiscard, gnu::hot, gnu::always_inline, gnu::malloc,
    gnu::alloc_size(2, 3)]]
  auto calloc(std::size_t count, std::size_t size) noexcept
    -> void* final override {
    if (size * count == 0) {
      return &zero_size_area;
    }
    void* ptr = nullptr;
    if (actor_stats_) {
      size = allocation_tag::storage_size_for(count * size,
                                              Traits::default_alignment);
      count = 1;
    }
    ptr = Traits::calloc(count, size);
    if (ptr == nullptr) {
      return nullptr;
    }
    if (stats_ || actor_stats_) {
      const auto usable = Traits::usable_size(ptr);
      if (stats_) {
        stats_->note_allocation(usable);
      }
      if (actor_stats_) {
        const auto& [tag, data_ptr]
          = allocation_tag::create_at(ptr, Traits::default_alignment);
        actor_stats_->note_allocation(*tag, usable);
        return data_ptr;
      }
    }
    return ptr;
  }

  [[nodiscard, gnu::hot, gnu::always_inline, gnu::malloc, gnu::alloc_size(2, 3),
    gnu::alloc_align(4)]]
  auto calloc(std::size_t count, std::size_t size,
              std::align_val_t alignment) noexcept -> void* final override {
    if (alignment <= Traits::default_alignment) {
      return calloc(count, size);
    }
    if (size * count == 0) {
      return &zero_size_area;
    }
    void* ptr = nullptr;
    if (actor_stats_) {
      size = allocation_tag::storage_size_for(count * size, alignment);
      count = 1;
    }
    ptr = Traits::calloc_aligned(count, size, std::to_underlying(alignment));
    if (ptr == nullptr) {
      return nullptr;
    }
    if (stats_ || actor_stats_) {
      const auto usable = Traits::usable_size(ptr);
      if (stats_) {
        stats_->note_allocation(usable);
      }
      if (actor_stats_) {
        const auto& [tag, data_ptr] = allocation_tag::create_at(ptr, alignment);
        actor_stats_->note_allocation(*tag, usable);
        return data_ptr;
      }
    }
    return ptr;
  }

  [[nodiscard, gnu::hot, gnu::always_inline, gnu::malloc, gnu::alloc_size(3)]]
  auto reallocate(void* old_ptr, std::size_t new_size) noexcept
    -> void* final override {
    if (new_size == 0) {
      deallocate(old_ptr);
      return &zero_size_area;
    }
    if (not old_ptr or old_ptr == &zero_size_area) {
      return allocate(new_size);
    }
    if (actor_stats_) {
      new_size
        = allocation_tag::storage_size_for(new_size, Traits::default_alignment);
      const auto [old_storage_ptr, old_tag]
        = allocation_tag::get_storage_and_tag(old_ptr);
      TENZIR_ASSERT(old_tag.alignment == Traits::default_alignment);
      const auto old_size = Traits::usable_size(old_storage_ptr);
      void* const new_storage_ptr = Traits::realloc(old_storage_ptr, new_size);
      if (new_storage_ptr == nullptr) {
        return nullptr;
      }
      const auto actual_new_size = Traits::usable_size(new_storage_ptr);
      if (stats_) {
        stats_->note_reallocation(old_storage_ptr != new_storage_ptr, old_size,
                                  actual_new_size);
      }
      auto [new_tag, new_data_ptr] = allocation_tag::obtain_from(
        new_storage_ptr, Traits::default_alignment);
      actor_stats_->note_reallocation(*new_tag, old_size, actual_new_size);
      return new_data_ptr;
    } else if (stats_) {
      const auto old_size = Traits::usable_size(old_ptr);
      const auto new_ptr = Traits::realloc(old_ptr, new_size);
      if (new_ptr == nullptr) {
        return new_ptr;
      }
      const auto actual_new_size = Traits::usable_size(new_ptr);
      stats_->note_reallocation(old_ptr != new_ptr, old_size, actual_new_size);
      return new_ptr;
    }
    return Traits::realloc(old_ptr, new_size);
  }

  [[nodiscard, gnu::hot, gnu::always_inline, gnu::malloc, gnu::alloc_size(3),
    gnu::alloc_align(4)]]
  auto reallocate(void* old_ptr, std::size_t new_size,
                  std::align_val_t alignment) noexcept -> void* final override {
    if (alignment <= Traits::default_alignment) {
      return reallocate(old_ptr, new_size);
    }
    if (new_size == 0) {
      deallocate(old_ptr);
      return &zero_size_area;
    }
    if (not old_ptr or old_ptr == &zero_size_area) {
      return allocate(new_size, alignment);
    }
    if (actor_stats_) {
      new_size = allocation_tag::storage_size_for(new_size, alignment);
      const auto [old_storage_ptr, old_tag]
        = allocation_tag::get_storage_and_tag(old_ptr);
      TENZIR_ASSERT(old_tag.alignment == alignment);
      const auto old_size = Traits::usable_size(old_storage_ptr);
      void* const new_storage_ptr = Traits::realloc_aligned(
        old_storage_ptr, new_size, std::to_underlying(alignment));
      if (new_storage_ptr == nullptr) {
        return nullptr;
      }
      const auto actual_new_size = Traits::usable_size(new_storage_ptr);
      if (stats_) {
        stats_->note_reallocation(old_storage_ptr != new_storage_ptr, old_size,
                                  actual_new_size);
      }
      auto [new_tag, data_ptr]
        = allocation_tag::obtain_from(new_storage_ptr, alignment);
      actor_stats_->note_reallocation(*new_tag, old_size, actual_new_size);
      return data_ptr;
    } else if (stats_) {
      const auto old_size = Traits::usable_size(old_ptr);
      const auto new_ptr = Traits::realloc_aligned(
        old_ptr, new_size, std::to_underlying(alignment));
      if (new_ptr == nullptr) {
        return new_ptr;
      }
      const auto actual_new_size = Traits::usable_size(new_ptr);
      stats_->note_reallocation(old_ptr != new_ptr, old_size, actual_new_size);
      return new_ptr;
    }
    return Traits::realloc_aligned(old_ptr, new_size,
                                   std::to_underlying(alignment));
  }

  [[gnu::hot, gnu::always_inline]]
  auto deallocate(void* ptr) noexcept -> void final override {
    if (ptr == nullptr) {
      return;
    }
    if (ptr == &zero_size_area) {
      return;
    }
    void* storage_ptr = ptr;
    if (actor_stats_) {
      const auto [storage, tag] = allocation_tag::get_storage_and_tag(ptr);
      storage_ptr = storage;
      const auto usable = Traits::usable_size(storage_ptr);
      if (stats_) {
        stats_->note_deallocation(usable);
      }
      actor_stats_->note_deallocation(tag, usable);
    } else if (stats_) {
      const auto usable = Traits::usable_size(storage_ptr);
      stats_->note_deallocation(usable);
    }
    Traits::free(storage_ptr);
  }

  [[gnu::hot, gnu::always_inline]]
  auto size(const void* ptr) const noexcept -> std::size_t final override {
    return Traits::usable_size(ptr);
  }

  auto trim() noexcept -> void final override {
    Traits::trim();
  }

  [[nodiscard]]
  auto stats() const noexcept -> const struct stats& final override {
    return stats_ ? *stats_ : zero_stats;
  }

  [[nodiscard]] auto actor_stats() const noexcept -> actor_stats_read override {
    if (actor_stats_) {
      return actor_stats_->read();
    }
    return {};
  }

  [[nodiscard]]
  auto backend() const noexcept -> enum backend final override {
    return Traits::backend_value;
  }

  [[nodiscard]]
  auto backend_name() const noexcept -> std::string_view final override {
    return Traits::name;
  }

private:
  struct stats* const stats_{nullptr};
  detail::actor_stats<InternalTrait>* const actor_stats_{nullptr};
};

} // namespace detail

#if TENZIR_ALLOCATOR_HAS_JEMALLOC

namespace jemalloc {

#  if TENZIR_GCC
[[nodiscard, gnu::hot, gnu::malloc(je_tenzir_free), gnu::alloc_size(1),
  gnu::alloc_align(2)]]
#  elif TENZIR_CLANG
[[nodiscard, gnu::hot, gnu::malloc, gnu::alloc_size(1), gnu::alloc_align(2)]]
#  endif
/// Simple helper that switches the arguments for `alloc_aligned` for consistency.
auto je_tenzir_malloc_aligned(std::size_t size, std::size_t alignment) noexcept
  -> void*;

#  if TENZIR_GCC
[[nodiscard, gnu::hot, gnu::malloc(je_tenzir_free), gnu::alloc_size(2),
  gnu::alloc_align(3)]]
#  elif TENZIR_CLANG
[[nodiscard, gnu::hot, gnu::malloc, gnu::alloc_size(2), gnu::alloc_align(3)]]
#  endif
/// We fake our own `realloc_aligned`, as that does not exist in C or POSIX.
auto je_tenzir_realloc_aligned(void* ptr, std::size_t new_size,
                               std::size_t alignment) noexcept -> void*;

#  if TENZIR_GCC
[[nodiscard, gnu::hot, gnu::malloc(je_tenzir_free), gnu::alloc_size(1, 2),
  gnu::alloc_align(3)]]
#  elif TENZIR_CLANG
[[nodiscard, gnu::hot, gnu::malloc, gnu::alloc_size(1, 2), gnu::alloc_align(3)]]
#  endif
/// We fake our own `calloc_aligned`, as that does not exist in C or POSIX.
auto je_tenzir_calloc_aligned(std::size_t count, std::size_t size,
                              std::size_t alignment) noexcept -> void*;

/// Wrapper for usable_size that accepts const void*.
inline auto je_tenzir_malloc_usable_size_const(const void* ptr) noexcept
  -> std::size_t {
  return ::je_tenzir_malloc_usable_size(const_cast<void*>(ptr));
}

/// No-op trim for jemalloc (doesn't have a trim / collect).
inline auto trim_noop() noexcept -> void {
}

struct traits {
  static constexpr auto backend_value = backend::jemalloc;
  static constexpr std::string_view name = "jemalloc";
  static constexpr auto default_alignment = std::align_val_t{16};

  static constexpr auto malloc = &::je_tenzir_malloc;
  static constexpr auto malloc_aligned = &je_tenzir_malloc_aligned;
  static constexpr auto calloc = &::je_tenzir_calloc;
  static constexpr auto calloc_aligned = &je_tenzir_calloc_aligned;
  static constexpr auto realloc = &::je_tenzir_realloc;
  static constexpr auto realloc_aligned = &je_tenzir_realloc_aligned;
  static constexpr auto free = &::je_tenzir_free;
  static constexpr auto usable_size = &je_tenzir_malloc_usable_size_const;
  static constexpr auto trim = &trim_noop;
};

static_assert(memory::detail::allocator_trait<traits>);

using allocator = memory::detail::basic_allocator<traits, traits>;

static_assert(memory::detail::allocator<allocator>);

} // namespace jemalloc
#endif

#if TENZIR_ALLOCATOR_HAS_MIMALLOC
namespace mimalloc {

/// Trim wrapper that calls mi_collect.
inline auto trim_collect() noexcept -> void {
  ::mi_collect(true);
}

struct traits {
  static constexpr auto backend_value = backend::mimalloc;
  static constexpr std::string_view name = "mimalloc";
  static constexpr auto default_alignment = std::align_val_t{16};

  static constexpr auto malloc = &::mi_malloc;
  static constexpr auto malloc_aligned = &::mi_malloc_aligned;
  static constexpr auto calloc = &::mi_calloc;
  static constexpr auto calloc_aligned = &::mi_calloc_aligned;
  static constexpr auto realloc = &::mi_realloc;
  static constexpr auto realloc_aligned = &::mi_realloc_aligned;
  static constexpr auto free = &::mi_free;
  static constexpr auto usable_size = &::mi_malloc_usable_size;
  static constexpr auto trim = &trim_collect;
};

static_assert(memory::detail::allocator_trait<traits>);

using allocator = memory::detail::basic_allocator<traits, traits>;

static_assert(memory::detail::allocator<allocator>);

} // namespace mimalloc
#endif

#if TENZIR_ALLOCATOR_MAY_USE_SYSTEM
namespace system {

[[gnu::hot]] auto trim() noexcept -> void;

}
#endif

#if TENZIR_ALLOCATOR_HAS_SYSTEM
namespace system {

[[gnu::hot]]
/// Function that will call the systems `free`, regardless of our overrides.
auto native_free(void* ptr) noexcept -> void;

#  ifndef __clang__
[[nodiscard, gnu::hot, gnu::malloc(native_free), gnu::alloc_size(1)]]
#  else
[[nodiscard, gnu::hot, gnu::malloc, gnu::alloc_size(1)]]
#  endif
/// Function that will call the systems `malloc`, regardless of our overrides.
auto native_malloc(std::size_t size) noexcept -> void*;

#  ifndef __clang__
[[nodiscard, gnu::hot, gnu::malloc(native_free), gnu::alloc_size(1, 2)]]
#  else
[[nodiscard, gnu::hot, gnu::malloc, gnu::alloc_size(1, 2)]]
#  endif
/// Function that will call the systems `calloc`, regardless of our overrides.
auto native_calloc(std::size_t count, std::size_t size) noexcept -> void*;

#  ifndef __clang__
[[nodiscard, gnu::hot, gnu::malloc(native_free), gnu::alloc_size(2)]]
#  else
[[nodiscard, gnu::hot, gnu::malloc, gnu::alloc_size(2)]]
#  endif
/// Function that will call the systems `realloc`, regardless of our overrides.
auto native_realloc(void* ptr, std::size_t new_size) noexcept -> void*;

[[nodiscard, gnu::hot]]
/// Function that will call the systems `malloc_usable_size`, regardless of our
/// overrides.
auto native_malloc_usable_size(const void* ptr) noexcept -> std::size_t;

#  ifndef __clang__
[[nodiscard, gnu::hot, gnu::malloc(native_free), gnu::alloc_size(1),
  gnu::alloc_align(2)]]
#  else
[[nodiscard, gnu::hot, gnu::malloc, gnu::alloc_size(1), gnu::alloc_align(2)]]
#  endif
/// Simple helper that switches the arguments for `alloc_aligned` for consistency.
auto malloc_aligned(std::size_t size, std::size_t alignment) noexcept -> void*;

#  ifndef __clang__
[[nodiscard, gnu::hot, gnu::malloc(native_free), gnu::alloc_size(2),
  gnu::alloc_align(3)]]
#  else
[[nodiscard, gnu::hot, gnu::malloc, gnu::alloc_size(2), gnu::alloc_align(3)]]
#  endif
/// We fake our own `realloc_aligned`, as that does not exist in C or POSIX.
auto realloc_aligned(void* ptr, std::size_t new_size,
                     std::size_t alignment) noexcept -> void*;

#  ifndef __clang__
[[nodiscard, gnu::hot, gnu::malloc(native_free), gnu::alloc_size(1, 2),
  gnu::alloc_align(3)]]
#  else
[[nodiscard, gnu::hot, gnu::malloc, gnu::alloc_size(1, 2), gnu::alloc_align(3)]]
#  endif
/// We fake our own `calloc_aligned`, as that does not exist in C or POSIX.
auto calloc_aligned(std::size_t count, std::size_t size,
                    std::size_t alignment) noexcept -> void*;

struct traits {
  static constexpr auto backend_value = backend::system;
  static constexpr auto name = std::string_view{"system"};
  static constexpr auto default_alignment
    = std::align_val_t{__STDCPP_DEFAULT_NEW_ALIGNMENT__};

  static constexpr auto malloc = &native_malloc;
  static constexpr auto malloc_aligned = &system::malloc_aligned;
  static constexpr auto calloc = &native_calloc;
  static constexpr auto calloc_aligned = &system::calloc_aligned;
  static constexpr auto realloc = &native_realloc;
  static constexpr auto realloc_aligned = &system::realloc_aligned;
  static constexpr auto free = &native_free;
  static constexpr auto usable_size = &native_malloc_usable_size;
  static constexpr auto trim = &system::trim;
};

static_assert(memory::detail::allocator_trait<traits>);

using allocator = memory::detail::basic_allocator<traits, traits>;

static_assert(memory::detail::allocator<allocator>);

} // namespace system
#endif

[[gnu::const]]
/// Checks if stats collection is enabled for the specific component or in
/// general.
auto enable_stats(const char* env) noexcept -> bool;

[[gnu::const]]
/// Checks if actor stats collection is enabled for the specific component or in
/// general.
auto enable_actor_stats(const char* env) noexcept -> bool;

[[gnu::const]]
/// Gets the trim interval from the environment.
auto trim_interval() noexcept -> tenzir::duration;

#if TENZIR_SELECT_ALLOCATOR == TENZIR_SELECT_ALLOCATOR_RUNTIME

[[gnu::const]]
/// Checks if an allocator was requested for the specific component or a
/// non-specific one was set. Returns the default otherwise.
auto selected_backend(const char* env) noexcept -> backend;

#  define TENZIR_MAKE_ALLOCATOR(NAME, ENV_SUFFIX)                              \
    [[nodiscard, gnu::hot, gnu::const]] inline auto NAME() noexcept            \
      -> polymorphic_allocator& {                                              \
      constinit static auto stats_ = stats{};                                  \
      static auto actor_stats_ = detail::actor_stats<system::traits>{};        \
      static auto jemalloc_ = jemalloc::allocator{                             \
        enable_stats("TENZIR_ALLOC_STATS_" ENV_SUFFIX) ? &stats_ : nullptr,    \
        enable_actor_stats("TENZIR_ALLOC_ACTOR_STATS_" ENV_SUFFIX)             \
          ? &actor_stats_                                                      \
          : nullptr};                                                          \
      static auto mimalloc_ = mimalloc::allocator{                             \
        enable_stats("TENZIR_ALLOC_STATS_" ENV_SUFFIX) ? &stats_ : nullptr,    \
        enable_actor_stats("TENZIR_ALLOC_ACTOR_STATS_" ENV_SUFFIX)             \
          ? &actor_stats_                                                      \
          : nullptr};                                                          \
      static auto system_ = system::allocator{                                 \
        enable_stats("TENZIR_ALLOC_STATS_" ENV_SUFFIX) ? &stats_ : nullptr,    \
        enable_actor_stats("TENZIR_ALLOC_ACTOR_STATS_" ENV_SUFFIX)             \
          ? &actor_stats_                                                      \
          : nullptr};                                                          \
      static auto& instance = [] {                                             \
        switch (selected_backend("TENZIR_ALLOC_" ENV_SUFFIX)) {                \
          case backend::jemalloc:                                              \
            return static_cast<polymorphic_allocator&>(jemalloc_);             \
          case backend::mimalloc:                                              \
            return static_cast<polymorphic_allocator&>(mimalloc_);             \
          case backend::system:                                                \
            return static_cast<polymorphic_allocator&>(system_);               \
        }                                                                      \
        TENZIR_UNREACHABLE();                                                  \
      }();                                                                     \
      return instance;                                                         \
    }

#elif TENZIR_SELECT_ALLOCATOR == TENZIR_SELECT_ALLOCATOR_JEMALLOC

#  define TENZIR_MAKE_ALLOCATOR(NAME, ENV_SUFFIX)                              \
    [[nodiscard, gnu::hot, gnu::const]] inline auto NAME() noexcept            \
      -> jemalloc::allocator& {                                                \
      constinit static auto stats_ = stats{};                                  \
      static auto actor_stats_ = detail::actor_stats<jemalloc::traits>{};      \
      static auto instance = jemalloc::allocator{                              \
        enable_stats("TENZIR_ALLOC_STATS_" ENV_SUFFIX) ? &stats_ : nullptr,    \
        enable_actor_stats("TENZIR_ALLOC_ACTOR_STATS_" ENV_SUFFIX)             \
          ? &actor_stats_                                                      \
          : nullptr};                                                          \
      return instance;                                                         \
    }

#elif TENZIR_SELECT_ALLOCATOR == TENZIR_SELECT_ALLOCATOR_MIMALLOC

#  define TENZIR_MAKE_ALLOCATOR(NAME, ENV_SUFFIX)                              \
    [[nodiscard, gnu::hot, gnu::const]] inline auto NAME() noexcept            \
      -> mimalloc::allocator& {                                                \
      constinit static auto stats_ = stats{};                                  \
      static auto actor_stats_ = detail::actor_stats<mimalloc::traits>{};      \
      static auto instance = mimalloc::allocator{                              \
        enable_stats("TENZIR_ALLOC_STATS_" ENV_SUFFIX) ? &stats_ : nullptr,    \
        enable_actor_stats("TENZIR_ALLOC_ACTOR_STATS_" ENV_SUFFIX)             \
          ? &actor_stats_                                                      \
          : nullptr};                                                          \
      return instance;                                                         \
    }

#elif TENZIR_SELECT_ALLOCATOR == TENZIR_SELECT_ALLOCATOR_SYSTEM

#  define TENZIR_MAKE_ALLOCATOR(NAME, ENV_SUFFIX)                              \
    [[nodiscard, gnu::hot, gnu::const]] inline auto NAME() noexcept            \
      -> system::allocator& {                                                  \
      constinit static auto stats_ = stats{};                                  \
      static auto actor_stats_ = detail::actor_stats<system::traits>{};        \
      static auto instance = system::allocator{                                \
        enable_stats("TENZIR_ALLOC_STATS_" ENV_SUFFIX) ? &stats_ : nullptr,    \
        enable_actor_stats("TENZIR_ALLOC_ACTOR_STATS_" ENV_SUFFIX)             \
          ? &actor_stats_                                                      \
          : nullptr};                                                          \
      return instance;                                                         \
    }

#else

/// This dummy allocator only exists to make the memory stats compile without
/// issue.
struct dummy_allocator {
  static auto stats() noexcept -> const stats& {
    return detail::zero_stats;
  }
};

#  define TENZIR_MAKE_ALLOCATOR(NAME, ENV_SUFFIX)                              \
    [[nodiscard]] inline auto NAME() noexcept -> dummy_allocator& {            \
      constinit static auto instance = dummy_allocator{};                      \
      return instance;                                                         \
    }

#endif

/// The allocator used by the arrow memory pool, so for all arrow *buffers*.
TENZIR_MAKE_ALLOCATOR(arrow_allocator, "ARROW")
/// The allocator used by `operator new` and `operator delete`.
TENZIR_MAKE_ALLOCATOR(cpp_allocator, "CPP")
/// The allocator used by `malloc` and other C/POSIX allocation functions.
TENZIR_MAKE_ALLOCATOR(c_allocator, "C")

#undef TENZIR_MAKE_ALLOCATOR

} // namespace tenzir::memory

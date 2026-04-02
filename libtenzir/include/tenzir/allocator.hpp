//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include "tenzir/allocator_config.hpp"
#include "tenzir/execution_node_name_guard.hpp"
#include "tenzir/option.hpp"

#include <boost/unordered/unordered_flat_map.hpp>
#include <caf/abstract_actor.hpp>
#include <caf/logger.hpp>
#include <folly/SharedMutex.h>
#include <tsl/robin_map.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <concepts>
#include <cstddef>
#include <cstring>
#include <mutex>
#include <pthread.h>
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

inline auto write_error(const char* txt, std::size_t size) noexcept {
  write(STDERR_FILENO, txt, size);
}
inline auto write_error(const char* txt) noexcept {
  write_error(txt, std::strlen(txt));
}
inline auto write_error(std::string_view txt) noexcept {
  write_error(txt.data(), txt.size());
}
#define TENZIR_ALLOCATOR_STRINGIFY(x) TENZIR_ALLOCATOR_STRINGIFY2(x)
#define TENZIR_ALLOCATOR_STRINGIFY2(x) #x

#if TENZIR_ENABLE_ASSERTIONS
#  define TENZIR_ALLOCATOR_ASSERT(COND)                                        \
    if (not static_cast<bool>(COND)) {                                         \
      write_error("assertion '" #COND "' failed" __FILE__                      \
                  ":" TENZIR_ALLOCATOR_STRINGIFY(__LINE__) "\n");              \
      std::_Exit(EXIT_FAILURE);                                                \
    }                                                                          \
    (void)0
#else
#  define TENZIR_ALLOCATOR_ASSERT(COND) (void)0
#endif

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

  // Copy constructor for copying stats with atomics
  stats(const stats& other) noexcept
    : bytes_current{other.bytes_current.load(std::memory_order_relaxed)},
      bytes_cumulative{other.bytes_cumulative.load(std::memory_order_relaxed)},
      bytes_peak{other.bytes_peak.load(std::memory_order_relaxed)},
      num_calls{other.num_calls.load(std::memory_order_relaxed)},
      allocations_current{
        other.allocations_current.load(std::memory_order_relaxed)},
      allocations_cumulative{
        other.allocations_cumulative.load(std::memory_order_relaxed)},
      allocations_peak{other.allocations_peak.load(std::memory_order_relaxed)} {
  }

  // Default constructor
  stats() noexcept = default;

  // Copy assignment
  auto operator=(const stats& other) noexcept -> stats& {
    if (this == &other) {
      return *this;
    }
    bytes_current.store(other.bytes_current.load(std::memory_order_relaxed),
                        std::memory_order_relaxed);
    bytes_cumulative.store(
      other.bytes_cumulative.load(std::memory_order_relaxed),
      std::memory_order_relaxed);
    bytes_peak.store(other.bytes_peak.load(std::memory_order_relaxed),
                     std::memory_order_relaxed);
    num_calls.store(other.num_calls.load(std::memory_order_relaxed),
                    std::memory_order_relaxed);
    allocations_current.store(
      other.allocations_current.load(std::memory_order_relaxed),
      std::memory_order_relaxed);
    allocations_cumulative.store(
      other.allocations_cumulative.load(std::memory_order_relaxed),
      std::memory_order_relaxed);
    allocations_peak.store(
      other.allocations_peak.load(std::memory_order_relaxed),
      std::memory_order_relaxed);
    return *this;
  }

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
  return N - m + std::to_underlying(alignment) * (m > 0);
}

class alignment_t {
public:
  explicit(false) alignment_t(std::align_val_t v)
    : exponent_{
        static_cast<std::uint8_t>(std::countr_zero(std::to_underlying(v)))} {
  }

  auto value() const noexcept -> std::size_t {
    return 1u << exponent_;
  }

  friend auto operator==(const alignment_t&, const alignment_t&) -> bool
    = default;
  friend auto operator<=>(const alignment_t&, const alignment_t&)
    -> std::strong_ordering
    = default;

private:
  std::uint8_t exponent_;
};

class actor_identifier {
public:
  auto name() const noexcept -> std::string_view {
    /// If the last character is a null, the name may be shorter.
    if (storage_.back() == '\0') {
      return {storage_.data()};
    }
    /// Otherwise, all characters are part of the string.
    return {storage_.data(), storage_.size()};
  }

  friend auto
  operator<=>(const actor_identifier& lhs, const actor_identifier& rhs) noexcept
    -> std::strong_ordering {
    // Mask out the alignment byte (byte 15, bits 120-127) when comparing
    constexpr auto mask = (__uint128_t{1} << 120) - 1;
    const auto lhs_bits = std::bit_cast<__uint128_t>(lhs) & mask;
    const auto rhs_bits = std::bit_cast<__uint128_t>(rhs) & mask;
    return lhs_bits <=> rhs_bits;
  }

  friend auto operator==(const actor_identifier& lhs,
                         const actor_identifier& rhs) noexcept -> bool {
    // Mask out the alignment byte (byte 15, bits 120-127) when comparing
    constexpr auto mask = (__uint128_t{1} << 120) - 1;
    return (std::bit_cast<__uint128_t>(lhs) & mask)
           == (std::bit_cast<__uint128_t>(rhs) & mask);
  }

  [[nodiscard]] static auto current() noexcept -> actor_identifier {
    actor_identifier res;
    res.make_this_current();
    return res;
  }

  auto make_this_current() noexcept -> void {
    if (exec_node_name_guard::operator_type
        != exec_node_name_guard::type::none) {
      storage_ = exec_node_name_guard::operator_name;
      return;
    }

    if (auto* const aptr = caf::abstract_actor::current()) {
      const char* const name = aptr->name();
#ifndef __clang__
#  pragma GCC diagnostic push
#  pragma GCC diagnostic ignored "-Wstringop-truncation"
#endif
      std::strncpy(storage_.data(), name, storage_.size());
#ifndef __clang__
#  pragma GCC diagnostic pop
#endif
      return;
    }
    const auto thread = pthread_self();
    // We want to handle the case where thread names use all characters.
    // Unfortunately, the API insists on its null terminator, so we need to
    // use a temporary buffer that can accommodate it and then simply drop it
    // when copying into the actual storage
    std::array<char, 16> storage = {};
    pthread_getname_np(thread, storage.data(), storage.size());
    std::memcpy(storage_.data(), storage.data(), storage_.size());
  }

  auto alignment() const noexcept -> alignment_t {
    return alignment_;
  }

  [[nodiscard]] auto as_comparable() const noexcept -> __uint128_t {
    // Mask out the alignment byte (byte 15, bits 120-127) when comparing
    constexpr static auto mask = (__uint128_t{1} << 120) - 1;
    return std::bit_cast<__uint128_t>(*this) & mask;
  }

private:
  std::array<char, 15> storage_
    = {'u', 'n', 'k', 'n', 'o', 'w', 'n'}; // make sure this is 0 initialized.
  alignment_t alignment_{std::align_val_t{1}};

  friend struct allocation_tag;
};

/// A tag placed at the beginning of an allocation
// [padding][tag][data...]
// ^storage_ptr
//               ^data_ptr
// This relies on tag always being right up against the data section. This is
// required to allow us to get from the data pointer to the tag pointer. The tag
// then contains the allocations alignment, which we need to get back to the
// storage pointer.
struct allocation_tag {
  actor_identifier source_identifier;

  struct tag_and_data {
    allocation_tag* tag_ptr;
    void* data_ptr;
  };

  static auto
  storage_size_for(std::size_t data_size, std::align_val_t alignment) noexcept
    -> std::size_t {
    TENZIR_ALLOCATOR_ASSERT(std::to_underlying(alignment)
                            >= sizeof(allocation_tag));
    return data_size + std::to_underlying(alignment);
  }

  static auto obtain_from(void* storage_ptr,
                          std::align_val_t alignment) noexcept -> tag_and_data {
    TENZIR_ALLOCATOR_ASSERT(std::to_underlying(alignment)
                            >= sizeof(allocation_tag));
    const auto uint_storage_ptr = reinterpret_cast<std::uintptr_t>(storage_ptr);
    void* const data_ptr = reinterpret_cast<void*>(
      uint_storage_ptr + std::to_underlying(alignment));
    allocation_tag* const tag_ptr = reinterpret_cast<allocation_tag*>(
      uint_storage_ptr + std::to_underlying(alignment)
      - sizeof(allocation_tag));
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
    res.tag_ptr->source_identifier.make_this_current();
    res.tag_ptr->source_identifier.alignment_ = alignment;
    return res;
  }

  struct cstorage_and_tag {
    const void* storage_ptr;
    const allocation_tag& tag;
  };

  struct storage_and_tag {
    void* storage_ptr;
    const allocation_tag& tag;
  };

  /// Gets both the storage pointer and tag from a data pointer
  [[nodiscard]] static auto get_storage_and_tag(const void* data_ptr) noexcept
    -> cstorage_and_tag {
    const auto uint_data_ptr = reinterpret_cast<std::uintptr_t>(data_ptr);
    const auto* const tag_ptr = reinterpret_cast<const allocation_tag*>(
      uint_data_ptr - sizeof(allocation_tag));
    const auto& tag = *tag_ptr;
    const void* const storage_ptr = reinterpret_cast<const void*>(
      uint_data_ptr - tag.source_identifier.alignment().value());
    return {
      .storage_ptr = storage_ptr,
      .tag = tag,
    };
  }
  /// Gets both the storage pointer and tag from a data pointer
  [[nodiscard]] static auto get_storage_and_tag(void* data_ptr) noexcept
    -> storage_and_tag {
    const auto [storage_ptr, tag]
      = get_storage_and_tag(const_cast<const void*>(data_ptr));
    return {.storage_ptr = const_cast<void*>(storage_ptr), .tag = tag};
  }
};

static_assert(sizeof(allocation_tag) == 16);
static_assert(alignof(allocation_tag) == 1);

struct actor_identifier_equal {
  static auto operator()(const actor_identifier& lhs,
                         const actor_identifier& rhs) noexcept -> bool {
    return lhs.as_comparable() == rhs.as_comparable();
  }
};

struct actor_identifier_hash {
  static auto operator()(const actor_identifier& name) noexcept -> std::size_t {
    return std::hash<__uint128_t>{}(name.as_comparable());
  }
};

using actor_stats_allocator
  = std::pmr::polymorphic_allocator<std::pair<const actor_identifier, stats>>;

using actor_stats_map
  = boost::unordered_flat_map<actor_identifier, stats, actor_identifier_hash,
                              actor_identifier_equal, actor_stats_allocator>;

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
  { t.actor_stats() } noexcept -> std::same_as<actor_stats_map>;
  { t.has_actor_stats() } noexcept -> std::same_as<bool>;
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

/// Simple per-entity tracking of memory. There is a map protected by a mutex,
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
    const auto success = try_note_impl(tag, &stats::note_allocation, size);
    if (success) {
      return;
    }
    const auto write_lock = std::scoped_lock{mut_};
    const auto [it, _]
      = allocations_by_entity_.try_emplace(tag.source_identifier);
    auto& value = it->second;
    value.note_allocation(size);
  }

  auto note_reallocation(const detail::allocation_tag& tag,
                         std::int64_t old_size, std::int64_t new_size) -> void {
    const auto success = try_note_impl(tag, &stats::note_reallocation, false,
                                       old_size, new_size);
    TENZIR_ALLOCATOR_ASSERT(success && "unexpected unknown reallocation");
  }

  auto note_deallocation(const detail::allocation_tag& tag, std::int64_t size)
    -> void {
    const auto success = try_note_impl(tag, &stats::note_deallocation, size);
    TENZIR_ALLOCATOR_ASSERT(success && "unexpected unknown deallocation");
  }

  /// Obtains the internal data as a copy
  [[nodiscard]] auto read() -> actor_stats_map {
    const auto lock = std::shared_lock{mut_};
    return {allocations_by_entity_, &resource_};
  }

  [[nodiscard]] auto is_known(const detail::allocation_tag& tag) -> bool {
    auto read_lock = std::shared_lock{mut_};
    auto it = allocations_by_entity_.find(tag.source_identifier);
    return it != allocations_by_entity_.end();
  }

private:
  template <class... Args>
  auto try_note_impl(const detail::allocation_tag& tag,
                     void (stats::*f)(Args...), Args... args) -> bool {
    auto read_lock = std::shared_lock{mut_};
    auto it = allocations_by_entity_.find(tag.source_identifier);
    if (it == allocations_by_entity_.end()) {
      return false;
    }
    auto& value = it->second;
    (value.*f)(args...);
    return true;
  }

  static inline basic_pmr_resource<InternalTrait> resource_;
  folly::SharedMutex mut_;
  actor_stats_map allocations_by_entity_{actor_stats_allocator{&resource_}};
};

template <detail::allocator_trait InternalTrait>
union actor_stats_storage {
  actor_stats<InternalTrait> value{};

  constexpr actor_stats_storage() noexcept = default;

  ~actor_stats_storage() {
  }
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
  virtual auto actor_stats() const noexcept -> detail::actor_stats_map = 0;
  virtual auto has_actor_stats() const noexcept -> bool = 0;
  virtual auto backend() const noexcept -> enum backend = 0;
  virtual auto backend_name() const noexcept -> std::string_view = 0;

private:
  friend auto destroy_allocator_actor_stats_on_shutdown() noexcept -> void;

  virtual auto destroy_actor_stats() noexcept -> void = 0;
};

static_assert(detail::allocator<polymorphic_allocator>);

auto destroy_allocator_actor_stats_on_shutdown() noexcept -> void;

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
    struct stats* stats, detail::actor_stats<InternalTrait>* actor_stats,
    std::string_view allocator_type)
    : stats_{stats},
      actor_stats_{actor_stats},
      allocator_type_{allocator_type} {
  }

  static_assert(sizeof(allocation_tag)
                  <= std::to_underlying(Traits::default_alignment),
                "The `allocation_tag´ implementation assumes that a tag fits "
                "into the width of the default alignment.");

  [[nodiscard, gnu::hot, gnu::always_inline, gnu::malloc, gnu::alloc_size(2)]]
  auto allocate(std::size_t size) noexcept -> void* final {
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
    -> void* final {
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
  auto calloc(std::size_t count, std::size_t size) noexcept -> void* final {
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
              std::align_val_t alignment) noexcept -> void* final {
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
  auto reallocate(void* old_ptr, std::size_t new_size) noexcept -> void* final {
    if (new_size == 0) {
      deallocate(old_ptr);
      return &zero_size_area;
    }
    if (not old_ptr or old_ptr == &zero_size_area) {
      return allocate(new_size);
    }
    if (actor_stats_) {
      const auto [old_storage_ptr, old_tag]
        = allocation_tag::get_storage_and_tag(old_ptr);
      if (not actor_stats_->is_known(old_tag)) {
        write_unknown_pointer_warning("reallocate", old_tag);
        return nullptr;
      }
      new_size
        = allocation_tag::storage_size_for(new_size, Traits::default_alignment);
      TENZIR_ALLOCATOR_ASSERT(old_tag.source_identifier.alignment()
                              == Traits::default_alignment);
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
    }
    if (stats_) {
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
                  std::align_val_t alignment) noexcept -> void* final {
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
      const auto [old_storage_ptr, old_tag]
        = allocation_tag::get_storage_and_tag(old_ptr);
      if (not actor_stats_->is_known(old_tag)) {
        write_unknown_pointer_warning("reallocate", old_tag);
        return nullptr;
      }
      new_size = allocation_tag::storage_size_for(new_size, alignment);
      TENZIR_ALLOCATOR_ASSERT(old_tag.source_identifier.alignment()
                              == alignment);
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
    }
    if (stats_) {
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
  auto deallocate(void* ptr) noexcept -> void final {
    if (ptr == nullptr) {
      return;
    }
    if (ptr == &zero_size_area) {
      return;
    }
    void* storage_ptr = ptr;
    if (actor_stats_) {
      const auto [storage, tag] = allocation_tag::get_storage_and_tag(ptr);
      if (not actor_stats_->is_known(tag)) {
        write_unknown_pointer_warning("deallocate", tag);
        return;
      }
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
  auto size(const void* ptr) const noexcept -> std::size_t final {
    if (ptr == nullptr or ptr == &zero_size_area) {
      return 0;
    }
    if (actor_stats_) {
      const auto [storage_ptr, tag] = allocation_tag::get_storage_and_tag(ptr);
      if (not actor_stats_->is_known(tag)) {
        write_unknown_pointer_warning("size", tag);
        return 0;
      }
      return Traits::usable_size(storage_ptr);
    }
    return Traits::usable_size(ptr);
  }

  auto trim() noexcept -> void final {
    Traits::trim();
  }

  [[nodiscard]]
  auto stats() const noexcept -> const struct stats& final {
    return stats_ ? *stats_ : zero_stats;
  }

  [[nodiscard]] auto actor_stats() const noexcept -> actor_stats_map override {
    if (actor_stats_) {
      return actor_stats_->read();
    }
    return {};
  }

  [[nodiscard]] auto has_actor_stats() const noexcept -> bool override {
    return actor_stats_ != nullptr;
  }

  [[nodiscard]]
  auto backend() const noexcept -> enum backend final {
    return Traits::backend_value;
  }

  [[nodiscard]]
  auto backend_name() const noexcept -> std::string_view final {
    return Traits::name;
  }

private:
  auto write_unknown_pointer_warning(std::string_view operation,
                                     const allocation_tag& tag) const noexcept
    -> void {
    auto buffer = std::array<char, 192>{};
    auto used = size_t{0};
    auto append = [&](std::string_view text) {
      TENZIR_ALLOCATOR_ASSERT(used + text.size() <= buffer.size());
      const auto free_size = buffer.size() - used;
      const auto copy_size = std::min(free_size, text.size());
      std::memcpy(buffer.data() + used, text.data(), copy_size);
      used += copy_size;
    };
    auto append_hex = [&](std::byte byte) {
      constexpr auto hex_digits = std::string_view{"0123456789abcdef"};
      if (used + 2 > buffer.size()) {
        return false;
      }
      const auto value = std::to_integer<std::uint8_t>(byte);
      buffer[used++] = hex_digits[value >> 4];
      buffer[used++] = hex_digits[value & 0x0f];
      return true;
    };
    const auto name = tag.source_identifier.name();
    const auto raw_identifier
      = std::bit_cast<std::array<std::byte, sizeof(actor_identifier)>>(
        tag.source_identifier);
    append("WARNING: allocator actor stats: unknown tag '");
    append(name);
    append("' rem=");
    for (auto i = name.size(); i < raw_identifier.size(); ++i) {
      if (i != name.size()) {
        if (used + 1 > buffer.size()) {
          break;
        }
        buffer[used++] = ':';
      }
      if (not append_hex(raw_identifier[i])) {
        break;
      }
    }
    append(" op=");
    append(operation);
    append(" cat=");
    append(allocator_type_);
    append(" backend=");
    append(Traits::name);
    append("\n");
    write_error(buffer.data(), used);
  }

  struct stats* const stats_{nullptr};
  detail::actor_stats<InternalTrait>* const actor_stats_{nullptr};
  const std::string_view allocator_type_;

  auto destroy_actor_stats() noexcept -> void final {
    if (actor_stats_ == nullptr) {
      return;
    }
    using actor_stats_type = detail::actor_stats<InternalTrait>;
    actor_stats_->~actor_stats_type();
  }
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
  static constexpr auto default_alignment = std::align_val_t{
    16}; // This is the default value on all system we build for. It could be
         // configured differently when building jemalloc, but we assume that is
         // not the case.

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
  static constexpr auto default_alignment
    = std::align_val_t{16}; // According to the docs, this is the default value
                            // (in like with libc malloc)

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

#  ifdef TENZIR_ALLOCATOR_HAS_JEMALLOC
#    define TENZIR_ALLOCATOR_JEMALLOC_INSTANCE(ENV_SUFFIX)                     \
      static auto jemalloc_                                                    \
        = detail::basic_allocator<jemalloc::traits, system::traits> {          \
        enable_stats("TENZIR_ALLOC_STATS_" ENV_SUFFIX) ? &stats_ : nullptr,    \
          enable_actor_stats("TENZIR_ALLOC_ACTOR_STATS_" ENV_SUFFIX)           \
            ? &actor_stats_                                                    \
            : nullptr,                                                         \
          ENV_SUFFIX                                                           \
      }
#    define TENZIR_ALLOCATOR_JEMALLOC_CASE()                                   \
      case backend::jemalloc:                                                  \
        return static_cast<polymorphic_allocator&>(jemalloc_)
#  else
#    define TENZIR_ALLOCATOR_JEMALLOC_INSTANCE() (void)0
#    define TENZIR_ALLOCATOR_JEMALLOC_CASE()                                   \
      case backend::jemalloc:                                                  \
        __builtin_unreachable()
#  endif

#  ifdef TENZIR_ALLOCATOR_HAS_MIMALLOC
#    define TENZIR_ALLOCATOR_MIMALLOC_INSTANCE(ENV_SUFFIX)                     \
      static auto mimalloc_                                                    \
        = detail::basic_allocator<mimalloc::traits, system::traits> {          \
        enable_stats("TENZIR_ALLOC_STATS_" ENV_SUFFIX) ? &stats_ : nullptr,    \
          enable_actor_stats("TENZIR_ALLOC_ACTOR_STATS_" ENV_SUFFIX)           \
            ? &actor_stats_                                                    \
            : nullptr,                                                         \
          ENV_SUFFIX                                                           \
      }
#    define TENZIR_ALLOCATOR_MIMALLOC_CASE()                                   \
      case backend::mimalloc:                                                  \
        return static_cast<polymorphic_allocator&>(mimalloc_)
#  else
#    define TENZIR_ALLOCATOR_MIMALLOC_INSTANCE() (void)0
#    define TENZIR_ALLOCATOR_MIMALLOC_CASE()                                   \
      case backend::mimalloc:                                                  \
        __builtin_unreachable()
#  endif

#  define TENZIR_MAKE_ALLOCATOR(NAME, ENV_SUFFIX)                              \
    [[nodiscard, gnu::hot, gnu::const]] inline auto NAME() noexcept            \
      -> polymorphic_allocator& {                                              \
      constinit static auto stats_ = stats{};                                  \
      static auto actor_stats_                                                 \
        = detail::actor_stats_storage<system::traits>{};                       \
      static auto system_ = system::allocator{                                 \
        enable_stats("TENZIR_ALLOC_STATS_" ENV_SUFFIX) ? &stats_ : nullptr,    \
        enable_actor_stats("TENZIR_ALLOC_ACTOR_STATS_" ENV_SUFFIX)             \
          ? &actor_stats_.value                                                \
          : nullptr,                                                           \
        ENV_SUFFIX};                                                           \
      TENZIR_ALLOCATOR_JEMALLOC_INSTANCE(ENV_SUFFIX);                          \
      TENZIR_ALLOCATOR_MIMALLOC_INSTANCE(ENV_SUFFIX);                          \
      static auto& instance = []() -> polymorphic_allocator& {                 \
        switch (selected_backend("TENZIR_ALLOC_" ENV_SUFFIX)) {                \
          TENZIR_ALLOCATOR_JEMALLOC_CASE();                                    \
          TENZIR_ALLOCATOR_MIMALLOC_CASE();                                    \
          case backend::system:                                                \
            return static_cast<polymorphic_allocator&>(system_);               \
        }                                                                      \
        __builtin_unreachable();                                               \
      }();                                                                     \
      return instance;                                                         \
    }

#elif TENZIR_SELECT_ALLOCATOR == TENZIR_SELECT_ALLOCATOR_JEMALLOC

#  define TENZIR_MAKE_ALLOCATOR(NAME, ENV_SUFFIX)                              \
    [[nodiscard, gnu::hot, gnu::const]] inline auto NAME() noexcept            \
      -> jemalloc::allocator& {                                                \
      constinit static auto stats_ = stats{};                                  \
      static auto actor_stats_                                                 \
        = detail::actor_stats_storage<jemalloc::traits>{};                     \
      static auto instance = jemalloc::allocator{                              \
        enable_stats("TENZIR_ALLOC_STATS_" ENV_SUFFIX) ? &stats_ : nullptr,    \
        enable_actor_stats("TENZIR_ALLOC_ACTOR_STATS_" ENV_SUFFIX)             \
          ? &actor_stats_.value                                                \
          : nullptr,                                                           \
        ENV_SUFFIX};                                                           \
      return instance;                                                         \
    }

#elif TENZIR_SELECT_ALLOCATOR == TENZIR_SELECT_ALLOCATOR_MIMALLOC

#  define TENZIR_MAKE_ALLOCATOR(NAME, ENV_SUFFIX)                              \
    [[nodiscard, gnu::hot, gnu::const]] inline auto NAME() noexcept            \
      -> mimalloc::allocator& {                                                \
      constinit static auto stats_ = stats{};                                  \
      static auto actor_stats_                                                 \
        = detail::actor_stats_storage<mimalloc::traits>{};                     \
      static auto instance = mimalloc::allocator{                              \
        enable_stats("TENZIR_ALLOC_STATS_" ENV_SUFFIX) ? &stats_ : nullptr,    \
        enable_actor_stats("TENZIR_ALLOC_ACTOR_STATS_" ENV_SUFFIX)             \
          ? &actor_stats_.value                                                \
          : nullptr,                                                           \
        ENV_SUFFIX};                                                           \
      return instance;                                                         \
    }

#elif TENZIR_SELECT_ALLOCATOR == TENZIR_SELECT_ALLOCATOR_SYSTEM

#  define TENZIR_MAKE_ALLOCATOR(NAME, ENV_SUFFIX)                              \
    [[nodiscard, gnu::hot, gnu::const]] inline auto NAME() noexcept            \
      -> system::allocator& {                                                  \
      constinit static auto stats_ = stats{};                                  \
      static auto actor_stats_                                                 \
        = detail::actor_stats_storage<system::traits>{};                       \
      static auto instance = system::allocator{                                \
        enable_stats("TENZIR_ALLOC_STATS_" ENV_SUFFIX) ? &stats_ : nullptr,    \
        enable_actor_stats("TENZIR_ALLOC_ACTOR_STATS_" ENV_SUFFIX)             \
          ? &actor_stats_.value                                                \
          : nullptr,                                                           \
        ENV_SUFFIX};                                                           \
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
#undef TENZIR_ALLOCATOR_MIMALLOC_INSTANCE
#undef TENZIR_ALLOCATOR_MIMALLOC_CASE
#undef TENZIR_ALLOCATOR_JEMALLOC_INSTANCE
#undef TENZIR_ALLOCATOR_JEMALLOC_CASE
#undef TENZIR_ALLOCATOR_STRINGIFY
#undef TENZIR_ALLOCATOR_STRINGIFY2
#undef TENZIR_ALLOCATOR_ASSERT

} // namespace tenzir::memory

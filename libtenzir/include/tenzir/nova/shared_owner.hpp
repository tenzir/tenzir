//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/allocator.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/nova/storage_fwd.hpp"
#include "tenzir/type_traits.hpp"

#include <atomic>
#include <functional>
#include <memory>
#include <utility>

namespace tenzir::nova::storage {

namespace _ {

/// `AllocFn` is the accessor of the allocator that owns the combined block,
/// e.g. `&memory::nova_data_allocator`.
template <class T, auto AllocFn>
struct Control {
  struct Deleter;

  std::atomic<size_t> strong_reference_count = 0;
  std::atomic<size_t> weak_reference_count = 0;
  ptrdiff_t element_count = 0;
  Deleter* storage_deleter = nullptr;

  struct Deleter {
    Deleter() = default;
    virtual auto deallocate(T* ptr) -> void = 0;
    Deleter(const Deleter&) = delete;
    Deleter(Deleter&&) = delete;
    auto operator=(const Deleter&) = delete;
    auto operator=(Deleter&&) = delete;
    virtual ~Deleter() = default;
  };

  auto is_shared() const noexcept -> bool {
    return strong_reference_count.load(std::memory_order_relaxed) > 1;
  }

  void free(T const* storage) {
    if (storage_deleter) {
      storage_deleter->deallocate(const_cast<T*>(storage));
      // NOLINTNEXTLINE
      delete storage_deleter;
    } else {
      std::destroy_n(storage, element_count);
    }
    std::destroy_at(this);
    AllocFn().deallocate(this);
  }
};

template <class T, auto AllocFn>
struct Allocation {
  Control<T, AllocFn>* control;
  T* data;
  Index actual_capacity;
};

/// Combined `[Control | padding | T[capacity]]` layout, one allocation
/// shared between the control block and the element storage.
template <class T, auto AllocFn>
constexpr auto padded_control_size() -> std::size_t {
  constexpr auto alignment = std::max(alignof(Control<T, AllocFn>), alignof(T));
  return (sizeof(Control<T, AllocFn>) + alignment - 1) / alignment * alignment;
}

template <class T, auto AllocFn>
auto allocation_size(Index capacity) -> std::size_t {
  return padded_control_size<T, AllocFn>()
         + static_cast<std::size_t>(capacity) * sizeof(T);
}

/// Fixes up the `Control*`/`T*` pair for a combined allocation starting at
/// `storage`. Does not construct or move anything; the caller is
/// responsible for the `Control` object itself.
template <class T, auto AllocFn>
auto layout(void* storage, Index capacity) -> Allocation<T, AllocFn> {
  auto* control_ptr = reinterpret_cast<Control<T, AllocFn>*>(storage);
  auto* data_ptr = reinterpret_cast<T*>(reinterpret_cast<std::byte*>(storage)
                                        + padded_control_size<T, AllocFn>());
  return {control_ptr, data_ptr, capacity};
}

/// Allocates a fresh combined block for `capacity` elements of `T` and
/// constructs the `Control` at its start. The control's element count
/// starts at zero and is marked unique.
template <class T, auto AllocFn>
auto allocate(Index capacity) -> Allocation<T, AllocFn> {
  TENZIR_ASSERT_GT(capacity, 0);
  static_assert(alignof(Control<T, AllocFn>) <= __STDCPP_DEFAULT_NEW_ALIGNMENT__
                  and alignof(T) <= __STDCPP_DEFAULT_NEW_ALIGNMENT__,
                "over-aligned types are not supported by plain malloc");
  auto* storage = AllocFn().allocate(allocation_size<T, AllocFn>(capacity));
  TENZIR_ASSERT(storage);
  auto result = layout<T, AllocFn>(storage, capacity);
  std::construct_at(result.control);
  result.control->strong_reference_count.store(1, std::memory_order_relaxed);
  result.control->element_count = 0;
  return result;
}

// Bytewise relocation must preserve the object representation and implement
// the same transfer as a trivial move, without requiring destruction.
template <class T>
inline constexpr auto can_reallocate
  = std::is_trivially_copyable_v<T>
    and std::is_trivially_move_constructible_v<T>;

/// Resizes a combined block via `realloc`, re-deriving the
/// `Control`/`T*` pair. Only valid for trivially relocatable `T`.
template <class T, auto AllocFn>
auto reallocate(Control<T, AllocFn>* control, Index new_capacity)
  -> Allocation<T, AllocFn> {
  static_assert(std::is_trivially_copyable_v<Control<T, AllocFn>>,
                "realloc requires a trivially copyable control block");
  static_assert(can_reallocate<T>,
                "nontrivial elements must be moved into a fresh allocation");
  TENZIR_ASSERT_GT(new_capacity, 0);
  TENZIR_ASSERT(not control->storage_deleter);
  auto* storage
    = AllocFn().reallocate(control, allocation_size<T, AllocFn>(new_capacity));
  TENZIR_ASSERT(storage);
  return layout<T, AllocFn>(storage, new_capacity);
}

} // namespace _

template <class T, auto AllocFn>
class SharedOwner {
  friend class SharedOwner<std::add_const_t<T>, AllocFn>;

public:
  constexpr SharedOwner() = default;
  constexpr SharedOwner(std::nullptr_t) noexcept : SharedOwner{} {
  }
  SharedOwner(const SharedOwner& other) noexcept : SharedOwner{} {
    copy_assign_from(other);
  }
  SharedOwner(const SharedOwner<std::remove_const_t<T>, AllocFn>& other)
    requires std::is_const_v<T>
    : SharedOwner{} {
    copy_assign_from(other);
  }

  SharedOwner(SharedOwner&& other) noexcept
    : control_{std::exchange(other.control_, nullptr)},
      data_{std::exchange(other.data_, nullptr)} {
  }
  SharedOwner& operator=(const SharedOwner& other) noexcept {
    copy_assign_from(other);
    return *this;
  }
  SharedOwner& operator=(SharedOwner&& other) noexcept {
    move_assign_from(std::move(other));
    return *this;
  }
  ~SharedOwner() {
    reset();
  }

  auto reset() -> void {
    if (not control_) {
      return;
    }
    const auto old_count = control_->strong_reference_count.fetch_sub(1);
    if (old_count == 1) {
      control_->free(data_);
    }
    control_ = nullptr;
    data_ = nullptr;
  }

  operator bool() const {
    return static_cast<bool>(this->data_);
  }

  template <typename Self>
  auto operator*(this Self&& self) -> ForwardLike<Self, T> {
    return std::forward_like<Self>(*self.data_);
  }

  auto operator->() const -> T* {
    return data_;
  }

  auto get() -> T* {
    return data_;
  }
  auto get() const -> T* {
    return data_;
  }

  auto as_unique() const& -> SharedOwner
    requires std::copy_constructible<T>
  {
    if (not control_) {
      return {};
    }
    return SharedOwner::make(*data_);
  }

  auto as_unique() && -> SharedOwner
    requires std::copy_constructible<T>
  {
    if (not is_shared()) {
      return std::move(*this);
    }
    return static_cast<SharedOwner const&>(*this).as_unique();
  }

  template <typename... Args>
    requires std::constructible_from<T, Args...>
  static auto make(Args&&... args) -> SharedOwner {
    auto [control_ptr, data_ptr, actual_capacity] = _::allocate<T, AllocFn>(1);
    data_ptr = std::construct_at(data_ptr, std::forward<Args>(args)...);
    control_ptr->element_count = 1;
    return {
      control_ptr,
      data_ptr,
    };
  }

  /// Ownership of the allocation passes to the returned SharedOwner. No
  /// external pointer, reference, or view into the allocation remains valid for
  /// use after this call. The deleter must destroy and release the object
  /// without throwing.
  template <class Deleter>
    requires(not std::is_const_v<T>
             and std::invocable<std::decay_t<Deleter>&, T*>)
  static auto adopt_mutable(T* pointer, Deleter&& deleter) -> SharedOwner {
    TENZIR_ASSERT(pointer);
    return adopt_mutable_impl(pointer, 1, std::forward<Deleter>(deleter));
  }

protected:
  using control_type = _::Control<std::remove_const_t<T>, AllocFn>;
  SharedOwner(control_type* control, T* data) noexcept
    : control_{control}, data_{data} {
  }

  template <class Deleter>
  static auto adopt_mutable_impl(T* pointer, Index count, Deleter&& deleter)
    -> SharedOwner {
    TENZIR_ASSERT_GEQ(count, 0);
    TENZIR_ASSERT(pointer or count == 0);
    struct ExternalDeleter final : control_type::Deleter {
      explicit ExternalDeleter(Deleter&& value)
        : value{std::forward<Deleter>(value)} {
      }
      auto deallocate(std::remove_const_t<T>* ptr) -> void override {
        std::invoke(value, ptr);
      }
      std::decay_t<Deleter> value;
    };
    auto* storage = AllocFn().allocate(sizeof(control_type));
    TENZIR_ASSERT(storage);
    auto* control = std::construct_at(static_cast<control_type*>(storage));
    control->strong_reference_count.store(1, std::memory_order_relaxed);
    control->element_count = count;
    // NOLINTNEXTLINE
    control->storage_deleter
      = new ExternalDeleter{std::forward<Deleter>(deleter)};
    return {control, pointer};
  }

  template <typename U>
    requires(std::same_as<std::add_const_t<T>, std::add_const_t<U>>)
  void copy_assign_from(const SharedOwner<U, AllocFn>& other) noexcept {
    if (static_cast<void const*>(this)
        == static_cast<void const*>(std::addressof(other))) {
      return;
    }
    reset();
    if (not other.control_) {
      return;
    }
    control_ = other.control_;
    data_ = other.data_;
    control_->strong_reference_count.fetch_add(1);
  }

  template <typename U>
    requires(std::same_as<std::add_const_t<T>, std::add_const_t<U>>)
  void move_assign_from(SharedOwner<U, AllocFn>&& other) noexcept {
    if (static_cast<void const*>(this)
        == static_cast<void const*>(std::addressof(other))) {
      return;
    }
    reset();
    if (not other.control_) {
      return;
    }
    control_ = std::exchange(other.control_, nullptr);
    data_ = std::exchange(other.data_, nullptr);
  }

  auto is_shared() const noexcept -> bool {
    return control_ and control_->is_shared();
  }

  control_type* control_ = nullptr;
  T* data_ = nullptr;
};

template <class T, auto AllocFn>
class SharedOwner<T[], AllocFn> : private SharedOwner<T, AllocFn> {
  using base = SharedOwner<T, AllocFn>;
  using base::control_;

  explicit SharedOwner(base owner) : base{std::move(owner)} {
  }

public:
  using base::base;
  using base::operator bool;
  using base::reset;

  SharedOwner() = default;

  /// Ownership of the allocation passes to the returned SharedOwner. No
  /// external pointer, reference, or view into the allocation remains valid for
  /// use after this call. Count is the number of initialized elements. The
  /// deleter must destroy and release the allocation without throwing, even for
  /// zero count.
  template <class Deleter>
    requires(not std::is_const_v<T>
             and std::invocable<std::decay_t<Deleter>&, T*>)
  static auto adopt_mutable(T* pointer, Index count, Deleter&& deleter)
    -> SharedOwner {
    auto owner = base::adopt_mutable_impl(pointer, count,
                                          std::forward<Deleter>(deleter));
    return SharedOwner{std::move(owner)};
  }

  auto as_unique() const& -> SharedOwner
    requires std::copy_constructible<T>;

  auto as_unique() && -> SharedOwner
    requires std::copy_constructible<T>;

  template <typename Self>
  [[gnu::always_inline]] auto operator[](this Self&& self, Index i)
    -> ForwardLike<Self, T> {
    TENZIR_ASSERT_EXPENSIVE(self.control_);
    TENZIR_ASSERT_LEQ_EXPENSIVE(0, i);
    TENZIR_ASSERT_LT_EXPENSIVE(i, self.control_->element_count);
    return std::forward_like<Self>(self.data_[i]);
  }

  auto length() const noexcept -> Index {
    return this->control_ ? static_cast<Index>(this->control_->element_count)
                          : 0;
  }

  auto begin() const noexcept -> T* {
    return this->data_;
  }

  auto end() const -> T* {
    return begin() + length();
  }

  auto back() -> T& {
    return operator[](static_cast<Index>(control_->element_count - 1));
  }

  class Builder {
  public:
    operator bool() const {
      return static_cast<bool>(owner_);
    }

    template <typename... Args>
      requires std::constructible_from<T, Args...>
    auto emplace_back(Args&&... args) -> T& {
      maybe_grow();
      auto& obj = *std::construct_at(owner_.end(), std::forward<Args>(args)...);
      owner_.control_->element_count += 1;
      return obj;
    }

    /// Appends `count` copies of `value` in one go. Reserves once and fills
    /// with a plain loop, so bulk null padding does not pay the per-element
    /// growth check of `emplace_back`.
    auto append_n(Index count, const T& value) -> void
      requires std::copy_constructible<T>
    {
      TENZIR_ASSERT_LEQ(0, count);
      if (count == 0) {
        return;
      }
      reserve_at_least(size() + count);
      std::uninitialized_fill_n(owner_.end(), count, value);
      owner_.control_->element_count += count;
    }

    auto move_append(T* begin, T* end) -> void {
      TENZIR_ASSERT_LEQ(begin, end);
      const auto count = end - begin;
      if (count == 0) {
        return;
      }
      reserve_at_least(size() + static_cast<Index>(count));
      std::uninitialized_move(begin, end, owner_.end());
      owner_.control_->element_count += count;
    }

    template <typename It>
    auto move_append(It begin, It end) -> void {
      const auto count = end - begin;
      if (count == 0) {
        return;
      }
      reserve_at_least(size() + static_cast<Index>(count));
      // Unlike the legacy algorithm, ranges::iter_move also handles iterators
      // that return prvalues, such as transform_view's, without dangling.
      std::ranges::uninitialized_move(begin, end, owner_.end(),
                                      owner_.end() + count);
      owner_.control_->element_count += count;
    }

    auto capacity() const -> Index {
      return capacity_;
    }

    auto operator[](Index i) -> T& {
      return owner_[i];
    }

    auto back() -> T& {
      TENZIR_ASSERT_GT_EXPENSIVE(size(), 0);
      return owner_[size() - 1];
    }

    auto pop_back() -> void {
      TENZIR_ASSERT_GT_EXPENSIVE(size(), 0);
      std::destroy_at(owner_.end() - 1);
      owner_.control_->element_count -= 1;
    }

    /// Drops every element at index `count` and beyond.
    auto truncate(Index count) -> void {
      TENZIR_ASSERT_LEQ(0, count);
      TENZIR_ASSERT_LEQ(count, size());
      std::destroy_n(owner_.data_ + count, size() - count);
      owner_.control_->element_count = count;
    }

    auto size() const -> Index {
      return owner_.length();
    }

    auto finish(bool trim = false) -> SharedOwner {
      if (trim) {
        shrink_to_fit();
      }
      capacity_ = 0;
      return std::move(owner_);
    }

    auto reset() -> void {
      owner_.reset();
      capacity_ = 0;
    }

    auto reserve_exact(Index N) -> void {
      if (N <= capacity_) {
        return;
      }
      if constexpr (_::can_reallocate<T>) {
        if (capacity_ > 0) {
          const auto element_count = owner_.control_->element_count;
          auto [control_ptr, data_ptr, actual_capacity]
            = _::reallocate<T, AllocFn>(owner_.control_, N);
          control_ptr->element_count = element_count;
          owner_.control_ = control_ptr;
          owner_.data_ = data_ptr;
          capacity_ = actual_capacity;
          return;
        }
      }
      auto new_alloc = SharedOwner::make_uninitialized(N);
      new_alloc.move_append(owner_.data_, owner_.data_ + owner_.length());
      *this = std::move(new_alloc);
    }

    auto reserve_at_least(Index N) -> void {
      if (N <= capacity_) {
        return;
      }
      reserve_exact(grow_size(N));
    }

    Builder() = default;
    Builder(const Builder&) = delete;
    Builder& operator=(const Builder&) = delete;
    Builder(Builder&& other) noexcept
      : owner_{std::exchange(other.owner_, nullptr)},
        capacity_{std::exchange(other.capacity_, 0)} {
    }
    Builder& operator=(Builder&& other) noexcept {
      owner_ = std::exchange(other.owner_, nullptr);
      capacity_ = std::exchange(other.capacity_, 0);
      return *this;
    }
    ~Builder() = default;

  private:
    auto shrink_to_fit() -> void {
      const auto element_count = size();
      if (element_count == capacity_) {
        return;
      }
      if (element_count == 0) {
        reset();
        return;
      }
      if constexpr (_::can_reallocate<T>) {
        auto [control_ptr, data_ptr, actual_capacity]
          = _::reallocate<T, AllocFn>(owner_.control_, element_count);
        control_ptr->element_count = element_count;
        owner_.control_ = control_ptr;
        owner_.data_ = data_ptr;
        capacity_ = actual_capacity;
        return;
      }
      auto result = SharedOwner::make_uninitialized(element_count);
      result.move_append(owner_.data_, owner_.data_ + element_count);
      *this = std::move(result);
    }

    auto grow_size(Index target) -> Index {
      auto const three_halves = (target * 3) / 2 + 1;
      return three_halves;
    }

    auto maybe_grow() -> void {
      if (size() < capacity_) {
        return;
      }
      if (capacity_ == 0) {
        reserve_exact(1);
        return;
      }
      reserve_exact(grow_size(capacity_));
    }

    Builder(SharedOwner owner, Index capacity)
      : owner_{std::move(owner)}, capacity_{capacity} {
    }
    friend class SharedOwner;

    SharedOwner owner_;
    Index capacity_ = 0;
  };

  static auto make_uninitialized(Index capacity) -> Builder {
    auto [control_ptr, data_ptr, actual_capacity]
      = _::allocate<T, AllocFn>(capacity);
    return {
      SharedOwner(control_ptr, data_ptr),
      actual_capacity,
    };
  }

  static auto make_value(Index count, const T& value) -> SharedOwner {
    auto [control_ptr, data_ptr, actual_capacity]
      = _::allocate<T, AllocFn>(count);
    for (Index i = 0; i < count; ++i) {
      std::construct_at(data_ptr + i, value);
    }
    control_ptr->element_count = count;
    return SharedOwner(control_ptr, data_ptr);
  }
};

template <class T, auto AllocFn>
auto SharedOwner<T[], AllocFn>::as_unique() const& -> SharedOwner
  requires std::copy_constructible<T>
{
  const auto count = length();
  if (count == 0) {
    return {};
  }
  // One allocation and one pass, without the per-element growth check of
  // `emplace_back`; for trivially copyable `T` this compiles to a `memcpy`.
  auto result = make_uninitialized(count);
  std::uninitialized_copy_n(begin(), count, result.owner_.end());
  result.owner_.control_->element_count = count;
  return std::move(result).finish();
}

template <class T, auto AllocFn>
auto SharedOwner<T[], AllocFn>::as_unique() && -> SharedOwner
  requires std::copy_constructible<T>
{
  if (not this->is_shared()) {
    return std::move(*this);
  }
  return static_cast<SharedOwner const&>(*this).as_unique();
}

/// Owns a column's backing storage.
template <class T>
using DataOwner = SharedOwner<T, &memory::nova_data_allocator>;

/// Owns the structure of an array, e.g. the fields of a record.
template <class T>
using StructureOwner = SharedOwner<T, &memory::nova_structure_allocator>;

} // namespace tenzir::nova::storage

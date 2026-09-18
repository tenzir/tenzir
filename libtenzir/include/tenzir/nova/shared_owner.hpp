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
#include <memory>
#include <utility>

namespace tenzir::nova::storage {

template <class T>
struct Control {
  struct Deleter;

  std::atomic<size_t> strong_reference_count = 0;
  std::atomic<size_t> weak_reference_count = 0;
  ptrdiff_t element_count = 0;
  Deleter* control_deleter = nullptr;
  Deleter* storage_deleter = nullptr;

  struct Deleter {
    virtual auto deallocate(void const* ptr) -> void {
      memory::nova_data_allocator().deallocate(const_cast<void*>(ptr));
    }
    virtual ~Deleter() = default;
  };

  auto is_shared() const noexcept -> bool {
    return strong_reference_count.load(std::memory_order_relaxed) > 1;
  }

  void free(T const* storage) const {
    std::destroy_n(storage, element_count);
    if (storage_deleter) {
      storage_deleter->deallocate(storage);
      // NOLINTNEXTLINE
      delete storage_deleter;
    }
    if (control_deleter) {
      auto* ctrl_del = control_deleter;
      control_deleter->deallocate(this);
      // NOLINTNEXTLINE
      delete ctrl_del;
    } else {
      Deleter{}.deallocate(this);
    }
  }
};

namespace shared_owner_detail {

template <typename T>
struct shared_owner_allocation {
  Control<T>* control;
  T* data;
  Index actual_capacity;
};

/// Combined `[Control<T> | padding | T[capacity]]` layout, one allocation
/// shared between the control block and the element storage.
template <typename T>
constexpr auto control_size() -> std::size_t {
  constexpr auto alignment = std::max(alignof(Control<T>), alignof(T));
  return (sizeof(Control<T>) + alignment - 1) / alignment * alignment;
}

template <typename T>
auto allocation_size(Index capacity) -> std::size_t {
  return control_size<T>() + static_cast<std::size_t>(capacity) * sizeof(T);
}

/// Fixes up the `Control<T>*`/`T*` pair for a combined allocation
/// starting at `storage`. Does not construct or move anything; the
/// caller is responsible for the `Control<T>` object itself.
template <typename T>
auto layout(void* storage, Index capacity) -> shared_owner_allocation<T> {
  auto* control_ptr = reinterpret_cast<Control<T>*>(storage);
  auto* data_ptr = reinterpret_cast<T*>(reinterpret_cast<std::byte*>(storage)
                                        + control_size<T>());
  return {control_ptr, data_ptr, capacity};
}

/// Allocates a fresh combined block for `capacity` elements of `T` and
/// constructs the `Control<T>` at its start. The control's element count
/// starts at zero and is marked unique.
template <typename T>
auto allocate(Index capacity) -> shared_owner_allocation<T> {
  TENZIR_ASSERT_GT(capacity, 0);
  static_assert(alignof(Control<T>) <= __STDCPP_DEFAULT_NEW_ALIGNMENT__
                  and alignof(T) <= __STDCPP_DEFAULT_NEW_ALIGNMENT__,
                "over-aligned types are not supported by plain malloc");
  auto* storage
    = memory::nova_data_allocator().allocate(allocation_size<T>(capacity));
  TENZIR_ASSERT(storage);
  auto result = layout<T>(storage, capacity);
  std::construct_at(result.control);
  result.control->strong_reference_count.store(1, std::memory_order_relaxed);
  result.control->element_count = 0;
  return result;
}

/// Grows a combined block in place via `realloc`, re-deriving the
/// `Control<T>`/`T*` pair. Only valid for trivially relocatable `T`.
template <typename T>
auto reallocate(Control<T>* control, Index new_capacity)
  -> shared_owner_allocation<T> {
  TENZIR_ASSERT_GT(new_capacity, 0);
  auto* storage = memory::nova_data_allocator().reallocate(
    control, allocation_size<T>(new_capacity));
  TENZIR_ASSERT(storage);
  return layout<T>(storage, new_capacity);
}

} // namespace shared_owner_detail

template <typename T>
class SharedOwner {
  friend class SharedOwner<std::add_const_t<T>>;

public:
  constexpr SharedOwner() = default;
  constexpr SharedOwner(std::nullptr_t) noexcept : SharedOwner{} {
  }
  SharedOwner(const SharedOwner& other) noexcept : SharedOwner{} {
    copy_assign_from(other);
  }
  SharedOwner(const SharedOwner<std::remove_const_t<T>>& other)
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
    auto [control_ptr, data_ptr, actual_capacity]
      = shared_owner_detail::allocate<T>(1);
    data_ptr = std::construct_at(data_ptr, std::forward<Args>(args)...);
    control_ptr->element_count = 1;
    return {
      control_ptr,
      data_ptr,
    };
  }

protected:
  using control_type = Control<std::remove_const_t<T>>;
  SharedOwner(control_type* control, T* data) noexcept
    : control_{control}, data_{data} {
  }

  template <typename U>
    requires(std::same_as<std::add_const_t<T>, std::add_const_t<U>>)
  void copy_assign_from(const SharedOwner<U>& other) noexcept {
    reset();
    if (not other) {
      return;
    }
    control_ = other.control_;
    data_ = other.data_;
    control_->strong_reference_count.fetch_add(1);
  }

  template <typename U>
    requires(std::same_as<std::add_const_t<T>, std::add_const_t<U>>)
  void move_assign_from(SharedOwner<U>&& other) noexcept {
    reset();
    if (not other) {
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

template <typename T>
class SharedOwner<T[]> : private SharedOwner<T> {
  using base = SharedOwner<T>;
  using base::control_;

public:
  using base::base;
  using base::operator bool;
  using base::reset;

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
      std::uninitialized_move(begin, end, owner_.end());
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
      constexpr static auto trivial = std::is_trivially_copy_constructible_v<T>
                                      and std::is_trivially_destructible_v<T>;
      if constexpr (trivial) {
        if (capacity_ > 0) {
          const auto element_count = owner_.control_->element_count;
          auto [control_ptr, data_ptr, actual_capacity]
            = shared_owner_detail::reallocate<T>(owner_.control_, N);
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
      constexpr static auto trivial = std::is_trivially_copy_constructible_v<T>
                                      and std::is_trivially_destructible_v<T>;
      if constexpr (trivial) {
        auto [control_ptr, data_ptr, actual_capacity]
          = shared_owner_detail::reallocate<T>(owner_.control_, element_count);
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
      = shared_owner_detail::allocate<T>(capacity);
    return {
      SharedOwner<T[]>(control_ptr, data_ptr),
      actual_capacity,
    };
  }

  static auto make_value(Index count, const T& value) -> SharedOwner {
    auto [control_ptr, data_ptr, actual_capacity]
      = shared_owner_detail::allocate<T>(count);
    for (Index i = 0; i < count; ++i) {
      std::construct_at(data_ptr + i, value);
    }
    control_ptr->element_count = count;
    return SharedOwner<T[]>(control_ptr, data_ptr);
  }
};

template <typename T>
auto SharedOwner<T[]>::as_unique() const& -> SharedOwner
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

template <typename T>
auto SharedOwner<T[]>::as_unique() && -> SharedOwner
  requires std::copy_constructible<T>
{
  if (not this->is_shared()) {
    return std::move(*this);
  }
  return static_cast<SharedOwner const&>(*this).as_unique();
}

} // namespace tenzir::nova::storage

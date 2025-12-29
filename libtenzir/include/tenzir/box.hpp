//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/detail/assert.hpp"

// FIXME: Don't include this here.
#include "tenzir/plugin.hpp"

#include <memory>
#include <type_traits>
#include <utility>

#pragma once

namespace tenzir {

// Forward declaration for the concept
template <class T>
class Box;

/// Concept to check if T has a copy() method returning Box<T>.
template <class T>
concept has_box_copy = requires(const T& t) {
  { t.copy() } -> std::same_as<Box<T>>;
};

/// A `std::unique_ptr` that is non-null and propagates `const`.
///
/// Whenever this object is passed around, it is assumed that it contains a
/// value. Moving from a box will still leave it in a "nullptr"-like state.
/// However, the resulting object is not considered valid and must not be used
/// afterwards. Furthermore, all access is checked for this invariant.
///
/// Box is copyable if T is copy-constructible and non-polymorphic, or if T
/// provides a copy() method returning Box<T>.
template <class T>
class Box {
public:
  /// Constructs a box from an existing non-null `unique_ptr`.
  static auto from_unique_ptr(std::unique_ptr<T> ptr) -> Box<T> {
    return Box{std::move(ptr)};
  }

  Box(Box&&) = default;
  Box& operator=(Box&&) = default;
  ~Box() = default;

  // FIXME: This is only default-constructible for CAF... Is that a good reason?
  Box() = default;

  /// Constructs a box from a value that is pointer-compatible with `T`.
  template <class U>
    requires std::convertible_to<std::unique_ptr<U>, std::unique_ptr<T>>
  explicit(false) Box(U x) : ptr_{std::make_unique<U>(std::move(x))} {
  }

  template <class... Args>
  explicit Box(std::in_place_t, Args&&... args)
    : ptr_{std::make_unique<T>(std::forward<Args>(args)...)} {
  }

  // TODO: Cleanup.
  Box(const Box& other) {
    if constexpr (std::is_copy_constructible_v<T>
                  && not std::is_polymorphic_v<T>) {
      ptr_ = std::make_unique<T>(*other);
    } else {
      static_assert(has_box_copy<T>);
      *this = other->copy();
    }
  }

  auto operator=(const Box& other) -> Box& {
    if (this != &other) {
      if constexpr (std::is_copy_constructible_v<T>
                    && not std::is_polymorphic_v<T>) {
        ptr_ = std::make_unique<T>(*other);
      } else {
        static_assert(has_box_copy<T>);
        *this = other->copy();
      }
    }
    return *this;
  }

  /// Boxes can be used as pointers.
  auto operator->() -> T* {
    return &deref();
  }
  auto operator->() const -> T const* {
    return &deref();
  }

  /// Dereferencing a box forwards the underlying object.
  template <class Self>
  auto operator*(this Self&& self) -> decltype(auto) {
    return std::forward<Self>(self).deref();
  }

  /// Boxes are implicitly convertible to references.
  explicit(false) operator T&() {
    return deref();
  }
  explicit(false) operator T const&() const {
    return deref();
  }
  explicit(false) operator T&&() && {
    return std::move(*this).deref();
  }

  /// Boxes are callable if the underlying type is.
  template <class Self, class... Args>
    requires std::is_invocable_v<T, Args...>
  auto operator()(this Self&& self, Args&&... args) -> decltype(auto) {
    return std::invoke(std::forward<Self>(self).deref(),
                       std::forward<Args>(args)...);
  }

  /// Comparing boxes compares their value.
  auto operator==(const Box& other) const -> bool {
    // TODO: Constraints.
    return *ptr_ == *other.ptr_;
  }

  friend auto inspect(auto& f, Box& x) -> bool {
    return tenzir::plugin_inspect(f, x.ptr_);
  }

private:
  template <class Self>
  auto deref(this Self&& self) -> decltype(auto) {
    TENZIR_ASSERT(self.ptr_);
    return std::forward_like<Self>(*self.ptr_);
  }

  explicit Box(std::unique_ptr<T> ptr) : ptr_{std::move(ptr)} {
    TENZIR_ASSERT(ptr_);
  }

  std::unique_ptr<T> ptr_;
};

template <class T>
Box(T) -> Box<T>;

} // namespace tenzir

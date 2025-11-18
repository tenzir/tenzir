//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <memory>
#include <utility>

#pragma once

namespace tenzir {

/// A `std::unique_ptr` that is non-null and propagates `const`.
///
/// Whenever this object is passed around, it is assumed that it contains a
/// value. Moving from a box will still leave it in a "nullptr"-like state.
/// However, the resulting object is not considered valid and must not be used
/// afterwards. Furthermore, all access is checked for this invariant.
template <class T>
class Box {
public:
  /// Constructs a box from an existing non-null `unique_ptr`.
  static auto from_unique_ptr(std::unique_ptr<T> ptr) -> Box<T> {
    return Box{std::move(ptr)};
  }

  // TODO: Is this a good idea?
  Box() : ptr_{std::make_unique<T>()} {
  }

  /// Constructs a box from a value that is pointer-compatible with `T`.
  template <class U>
    requires std::convertible_to<std::unique_ptr<U>, std::unique_ptr<T>>
  explicit(false) Box(U x) : ptr_{std::make_unique<U>(std::move(x))} {
  }

  template <class... Args>
  explicit Box(std::in_place_t, Args&&... args)
    : ptr_{std::make_unique<T>(std::forward<Args>(args)...)} {
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

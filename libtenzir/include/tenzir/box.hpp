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

/// A `std::unique_ptr` that is non-null.
///
/// Whenever this object is passed around, it is assumed that it contains a
/// value. Moving from a box will still leave it in a "nullptr"-like state.
/// However, the resulting object is not considered valid and must not be used
/// afterwards. Furthermore, all access is checked for this invariant.
template <class T>
class Box {
public:
  static auto from_non_null(std::unique_ptr<T> ptr) -> Box<T> {
    return Box{std::move(ptr)};
  }

  /// Constructs a box from a value that is pointer-compatible with `T`.
  template <class U>
    requires std::convertible_to<std::unique_ptr<U>, std::unique_ptr<T>>
  explicit(false) Box(U x) : ptr_{std::make_unique<U>(std::move(x))} {
  }

  template <class... Ts>
  explicit(false) Box(std::in_place_t, Ts&&... xs)
    : ptr_{std::make_unique<T>(std::forward<Ts>(xs)...)} {
  }

  auto operator->() -> T* {
    TENZIR_ASSERT(ptr_);
    return ptr_.get();
  }

  auto operator->() const -> T const* {
    TENZIR_ASSERT(ptr_);
    return ptr_.get();
  }

  auto operator*() -> T& {
    TENZIR_ASSERT(ptr_);
    return *ptr_;
  }

  auto operator*() const -> T const& {
    TENZIR_ASSERT(ptr_);
    return *ptr_;
  }

private:
  explicit Box(std::unique_ptr<T> ptr) : ptr_{std::move(ptr)} {
    TENZIR_ASSERT(ptr_);
  }

  std::unique_ptr<T> ptr_;
};

template <class T>
Box(T ptr) -> Box<T>;

} // namespace tenzir

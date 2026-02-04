//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/panic.hpp"
#include "tenzir/type_traits.hpp"

#include <memory>
#include <type_traits>
#include <utility>

namespace tenzir {

/// A non-copyable alternative to `std::any`.
class Any {
public:
  Any() noexcept = default;
  ~Any() = default;

  Any(Any&& other) noexcept = default;
  auto operator=(Any&& other) noexcept -> Any& = default;

  Any(Any const&) = delete;
  auto operator=(Any const&) -> Any& = delete;

  /// Constructs an Any holding a value of type `decay_t<T>`.
  template <class T>
    requires(not std::same_as<std::decay_t<T>, Any>)
  explicit(false) Any(T&& value)
    : ptr_{std::make_unique<Holder<std::decay_t<T>>>(std::forward<T>(value))} {
  }

  /// Constructs an Any holding a value of type `T` constructed in-place.
  template <class T, class... Args>
  explicit Any(std::in_place_type_t<T>, Args&&... args)
    : ptr_{std::make_unique<Holder<T>>(std::forward<Args>(args)...)} {
  }

  /// Returns a reference to the contained value.
  /// Panics if empty or if the type does not match.
  template <class T, class Self>
  auto as(this Self&& self) -> ForwardLike<Self, T> {
    if (auto* holder = dynamic_cast<Holder<T>*>(self.ptr_.get())) {
      return std::forward_like<Self>(holder->value);
    }
    if (not self.ptr_) {
      panic("as<{}>() called on empty Any", type_name<T>());
    }
    panic("as<{}>() called on Any with different type", type_name<T>());
  }

  /// Returns a pointer to the contained value, or nullptr if empty or type
  /// mismatch.
  template <class T>
  auto try_as() -> T* {
    if (auto* holder = dynamic_cast<Holder<T>*>(ptr_.get())) {
      return &holder->value;
    }
    return nullptr;
  }

  /// Returns a const pointer to the contained value, or nullptr if empty or
  /// type mismatch.
  template <class T>
  auto try_as() const -> T const* {
    if (auto* holder = dynamic_cast<Holder<T> const*>(ptr_.get())) {
      return &holder->value;
    }
    return nullptr;
  }

private:
  struct HolderBase {
    virtual ~HolderBase() = default;
  };

  template <class T>
  struct Holder final : HolderBase {
    T value;

    template <class... Args>
    explicit Holder(Args&&... args) : value{std::forward<Args>(args)...} {
    }
  };

  std::unique_ptr<HolderBase> ptr_;
};

} // namespace tenzir

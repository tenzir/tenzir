//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/error.hpp"

#include <memory>

namespace tenzir {

/// A `unique_ptr` that is copyable like a value.
///
/// @warning Do not use with polymorphic, non-final classes.
template <class T>
class value_ptr {
public:
  // TODO: Make instances of this class even more similar to `T`.

  value_ptr() : ptr_{std::make_unique<T>()} {
  }

  explicit(false) value_ptr(T x) : ptr_{std::make_unique<T>(std::move(x))} {
  }

  ~value_ptr() = default;

  value_ptr(const value_ptr& other)
    : ptr_{other.ptr_ ? std::make_unique<T>(*other.ptr_) : nullptr} {
  }

  value_ptr(value_ptr&&) noexcept = default;

  auto operator=(const value_ptr& other) -> value_ptr& {
    *this = value_ptr{other};
  }

  auto operator=(value_ptr&&) noexcept -> value_ptr& = default;

  explicit operator bool() const {
    return !!ptr_;
  }

  auto operator*() const -> T& {
    TENZIR_ASSERT_EXPENSIVE(*this);
    return *ptr_;
  }

  auto operator->() const -> T* {
    TENZIR_ASSERT_EXPENSIVE(*this);
    return &*ptr_;
  }

  /// Instances of this class are only inspectable if they contain a value.
  template <class Inspector>
  friend auto inspect(Inspector& f, value_ptr& x) {
    if constexpr (Inspector::is_loading) {
      x.ptr_ = std::make_unique<T>();
      return f.apply(*x);
    } else {
      if (!x.ptr_) {
        f.set_error(caf::make_error(ec::serialization_error,
                                    "inspecting a moved-from `value_ptr`"));
        return false;
      }
      return f.apply(*x);
    }
  }

  auto operator==(const value_ptr& other) const -> bool {
    const auto& self = *this;
    if (!self || !other) {
      return !self && !other;
    }
    return *self == *other;
  }

private:
  std::unique_ptr<T> ptr_;
};

} // namespace tenzir

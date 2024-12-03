//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <utility>

namespace tenzir::detail {

/// A lightweight scope guard implementation.
template <class Fun>
class [[nodiscard]] scope_guard {
public:
  static_assert(noexcept(std::declval<Fun>()()),
                "scope_guard requires a noexcept cleanup function");

  explicit scope_guard(Fun f) noexcept : fun_{std::move(f)}, enabled_{true} {
  }

  scope_guard() = delete;

  scope_guard(const scope_guard&) = delete;
  scope_guard(scope_guard&& other) noexcept
    : fun_{std::move(other.fun_)}, enabled_{other.enabled_} {
    other.enabled_ = false;
  }

  auto operator=(const scope_guard&) -> scope_guard& = delete;
  auto operator=(scope_guard&& other) noexcept -> scope_guard& {
    fun_ = std::move(other.fun_);
    enabled_ = std::move(other.enabled_);
    other.enabled_ = false;
  }

  ~scope_guard() noexcept {
    if (enabled_) {
      fun_();
    }
  }

  /// Disables this guard, i.e., the guard does not
  /// run its cleanup code as it goes out of scope.
  void disable() noexcept {
    enabled_ = false;
  }

private:
  Fun fun_;
  bool enabled_;
};

} // namespace tenzir::detail

//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <type_traits>
#include <utility>

namespace tenzir::detail {

/// A lightweight scope guard implementation.
template <class Fun>
  requires requires(Fun f) {
    { f() } noexcept -> std::same_as<void>;
  }
class [[nodiscard]] scope_guard {
public:
  explicit scope_guard(Fun f) noexcept : fun_{std::move(f)}, enabled_{true} {
  }

  scope_guard() = delete;

  static auto make_disengaged() noexcept -> scope_guard
    requires std::is_nothrow_default_constructible_v<Fun>
  {
    return scope_guard{std::in_place};
  }

  scope_guard(const scope_guard&) = delete;
  scope_guard(scope_guard&& other) noexcept
    : fun_{std::move(other.fun_)},
      enabled_{std::exchange(other.enabled_, false)} {
  }

  auto operator=(const scope_guard&) -> scope_guard& = delete;
  auto operator=(scope_guard&& other) noexcept -> scope_guard& {
    fun_ = std::exchange(other.fun_, {});
    enabled_ = std::exchange(other.enabled_, false);
    return *this;
  }

  auto operator=(Fun&& fun) noexcept -> scope_guard&
    requires std::is_nothrow_move_assignable_v<Fun>
  {
    fun_ = std::move(fun);
    enabled_ = true;
    return *this;
  }

  ~scope_guard() noexcept {
    trigger();
  }

  void trigger() noexcept {
    if (enabled_) {
      fun_();
      enabled_ = false;
    }
  }

  /// Disables this guard, i.e., the guard does not
  /// run its cleanup code as it goes out of scope.
  void disable() noexcept {
    enabled_ = false;
  }

private:
  explicit scope_guard(std::in_place_t)
    requires std::is_nothrow_default_constructible_v<Fun>
    : fun_{}, enabled_{false} {
  }

  Fun fun_;
  bool enabled_;
};

} // namespace tenzir::detail

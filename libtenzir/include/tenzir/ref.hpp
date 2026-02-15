//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <functional>
#include <type_traits>
#include <utility>

namespace tenzir {

/// A non-owning reference wrapper like `std::reference_wrapper`, but with
/// `operator->` and `operator*`.
template <class T>
class Ref {
public:
  explicit(false) Ref(T& value) : ptr_{&value} {
  }

  auto get() const -> T& {
    return *ptr_;
  }

  explicit(false) operator T&() const {
    return *ptr_;
  }

  auto operator->() const -> T* {
    return ptr_;
  }

  auto operator*() const -> T& {
    return *ptr_;
  }

  template <class... Args>
  auto operator()(Args&&... args) const -> std::invoke_result_t<T&, Args...> {
    return std::invoke(*ptr_, std::forward<Args>(args)...);
  }

private:
  T* ptr_;
};

} // namespace tenzir

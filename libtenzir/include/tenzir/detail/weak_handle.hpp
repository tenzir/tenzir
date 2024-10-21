//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include <caf/actor_cast.hpp>

namespace tenzir::detail {

/// A weak handle adaptor for CAF's typed actor handles. Essentially,
/// weak_handle<caf::typed_actor<T...>> is to caf::typed_actor<T...> what
/// std::weak_ptr<U> is to std::shared_ptr<U>.
///
/// Weak handles are implicitly constructible from strong handles, and
/// essentially only expose one functionality: lock, which acquires a strong
/// handle if possible. Here's how to use it:
///
///   weak_handle<my_actor> weak = ...;
///   if (auto handle = weak.lock()) {
///     do_something_with(handle);
///   }
///
template <class Handle>
struct weak_handle : caf::weak_actor_ptr {
  weak_handle() noexcept = default;
  weak_handle(const weak_handle&) noexcept = default;
  auto operator=(const weak_handle&) noexcept -> weak_handle& = default;
  weak_handle(weak_handle&&) noexcept = default;
  auto operator=(weak_handle&&) noexcept -> weak_handle& = default;
  ~weak_handle() noexcept = default;

  explicit(false) weak_handle(const Handle& handle) noexcept
    : weak_ptr_{handle->ctrl()} {
    // nop
  }

  auto lock() const noexcept -> Handle {
    return caf::actor_cast<Handle>(weak_ptr_.lock());
  }

  friend auto inspect(auto& f, weak_handle& x) -> bool {
    return f.object(x)
      .pretty_name("tenzir.detail.weak_handle")
      .fields(f.field("weak_ptr", x.weak_ptr_));
  }

private:
  caf::weak_actor_ptr weak_ptr_ = {};
};

} // namespace tenzir::detail

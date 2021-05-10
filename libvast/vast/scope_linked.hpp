//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <caf/actor.hpp>
#include <caf/exit_reason.hpp>
#include <caf/send.hpp>

namespace vast {

/// Links an actor to a scope by sending an exit message to the managed actor
/// on destruction.
template <class Handle>
class scope_linked {
public:
  // -- constructors, destructors, and assignment operators --------------------

  scope_linked() = default;

  explicit scope_linked(Handle hdl) : hdl_(std::move(hdl)) {
    // nop
  }

  scope_linked(scope_linked&&) = default;

  scope_linked(const scope_linked&) = default;

  scope_linked& operator=(scope_linked&&) = default;

  scope_linked& operator=(const scope_linked&) = default;

  ~scope_linked() {
    if (hdl_)
      caf::anon_send_exit(hdl_, caf::exit_reason::user_shutdown);
  }

  // -- properties -------------------------------------------------------------

  /// @returns the managed actor.
  [[nodiscard]] const Handle& get() const {
    return hdl_;
  }

private:
  // -- member variables -------------------------------------------------------

  /// The managed actor.
  Handle hdl_;
};

/// Explicit deduction guide for overload (not needed as of C++20).
template <class Handle>
scope_linked(Handle) -> scope_linked<Handle>;

} // namespace vast

/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

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
  const Handle& get() const {
    return hdl_;
  }

private:
  // -- member variables -------------------------------------------------------

  /// The managed actor.
  Handle hdl_;
};

using scope_linked_actor = scope_linked<caf::actor>;

} // namespace vast

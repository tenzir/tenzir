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

#include "vast/fwd.hpp"

#include <caf/meta/type_name.hpp>
#include <caf/typed_event_based_actor.hpp>

namespace vast::system {

/// A flush listener actor listens for flushes.
using flush_listener_actor = caf::typed_actor<
  // Reacts to the requested flush message.
  caf::reacts_to<atom::flush>>;

/// Contains a flush_listener_actor; this allows for sending them over the wire.
struct wrapped_flush_listener {
  flush_listener_actor actor;

  template <class Inspector>
  friend auto inspect(Inspector& f, wrapped_flush_listener& x) {
    return f(caf::meta::type_name("vast.system.wrapped_flush_listener"),
             x.actor);
  }
};

} // namespace vast::system

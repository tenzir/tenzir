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

#define SUITE scope_linked

#include "vast/scope_linked.hpp"

#include "test.hpp"
#include "fixtures/actor_system.hpp"

using namespace vast;

namespace {

caf::behavior dummy() {
  return {
    [] {
      // nop
    }
  };
}

} // namespace <anonymous>

FIXTURE_SCOPE(scope_linked_tests, fixtures::deterministic_actor_system)

TEST(exit message on exit) {
  // Spawn dummy, assign it to a scope_linked handle (sla) and make sure it
  // gets killed when sla goes out of scope.
  caf::actor hdl;
  { // "lifetime scope" for our dummy
    scope_linked_actor sla{sys.spawn(dummy)};
    // Store the actor handle in the outer scope, otherwise we can't check for
    // a message to the dummy.
    hdl = sla.get();
  }
  // The sla handle must send an exit_msg when going out of scope.
  expect((caf::exit_msg), from(_).to(hdl));
}

FIXTURE_SCOPE_END()

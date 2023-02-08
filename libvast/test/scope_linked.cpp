//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/scope_linked.hpp"

#include "vast/test/fixtures/actor_system.hpp"
#include "vast/test/test.hpp"

using namespace vast;

namespace {

caf::behavior dummy() {
  return {
    [] {
      // nop
    }
  };
}

struct fixture : public fixtures::deterministic_actor_system {
  fixture() : fixtures::deterministic_actor_system(VAST_PP_STRINGIFY(SUITE)) {
  }
};

} // namespace

FIXTURE_SCOPE(scope_linked_tests, fixture)

TEST(exit message on exit) {
  // Spawn dummy, assign it to a scope_linked handle (sla) and make sure it
  // gets killed when sla goes out of scope.
  caf::actor hdl;
  { // "lifetime scope" for our dummy
    scope_linked<caf::actor> sla{sys.spawn(dummy)};
    // Store the actor handle in the outer scope, otherwise we can't check for
    // a message to the dummy.
    hdl = sla.get();
  }
  // The sla handle must send an exit_msg when going out of scope.
  expect((caf::exit_msg), from(_).to(hdl));
}

FIXTURE_SCOPE_END()

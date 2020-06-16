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

#include "vast/system/wait.hpp"

#include "vast/fwd.hpp"

#include <caf/behavior.hpp>
#include <caf/event_based_actor.hpp>

#define SUITE terminator
#include "vast/test/fixtures/actor_system.hpp"
#include "vast/test/test.hpp"

using namespace vast;
using namespace vast::system;

namespace {

caf::behavior worker(caf::event_based_actor* self) {
  return [=](atom::done) { self->quit(); };
}

struct terminator_fixture : fixtures::actor_system {
  terminator_fixture() {
    victims = std::vector<caf::actor>{sys.spawn(worker), sys.spawn(worker),
                                      sys.spawn(worker)};
  }

  std::vector<caf::actor> victims;
};

} // namespace

FIXTURE_SCOPE(terminator_tests, terminator_fixture)

TEST(parallel shutdown) {
  wait<policy::parallel>(self, victims);
}

TEST(sequential shutdown) {
  wait<policy::sequential>(self, victims);
}

FIXTURE_SCOPE_END()

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

#include "vast/system/terminator.hpp"

#include "vast/system/atoms.hpp"

#include <caf/all.hpp>

#define SUITE terminator
#include "vast/test/fixtures/actor_system.hpp"
#include "vast/test/test.hpp"

using namespace vast;
using namespace vast::system;

namespace {

caf::behavior worker(caf::event_based_actor* self) {
  return [=](done_atom) { self->quit(); };
}

struct terminator_fixture : fixtures::actor_system {
  terminator_fixture() {
    victims = std::vector<caf::actor>{sys.spawn(worker), sys.spawn(worker),
                                      sys.spawn(worker)};
  }

  void run(const caf::actor& aut) {
    self->request(aut, caf::infinite, victims)
      .receive(
        [=](done_atom) { MESSAGE("terminated all actors successfully"); },
        [=](const caf::error&) {
          FAIL("could not terminate actors properly");
        });
  }

  std::vector<caf::actor> victims;
};

} // namespace

FIXTURE_SCOPE(terminator_tests, terminator_fixture)

TEST(parallel shutdown) {
  run(sys.spawn(terminator<policy::parallel>));
}

TEST(sequential shutdown) {
  run(sys.spawn(terminator<policy::sequential>));
}

FIXTURE_SCOPE_END()

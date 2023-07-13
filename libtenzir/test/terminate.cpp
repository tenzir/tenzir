//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/terminate.hpp"

#include "tenzir/fwd.hpp"

#include "tenzir/atoms.hpp"
#include "tenzir/test/fixtures/actor_system.hpp"
#include "tenzir/test/test.hpp"

#include <caf/behavior.hpp>
#include <caf/event_based_actor.hpp>

using namespace tenzir;
using namespace tenzir;

namespace {

caf::behavior worker(caf::event_based_actor* self) {
  return [=](atom::done) { self->quit(); };
}

struct fixture : fixtures::actor_system {
  fixture() : fixtures::actor_system(TENZIR_PP_STRINGIFY(SUITE)) {
    victims = std::vector<caf::actor>{sys.spawn(worker), sys.spawn(worker),
                                      sys.spawn(worker)};
  }

  std::vector<caf::actor> victims;
};

} // namespace

FIXTURE_SCOPE(terminate_tests, fixture)

TEST(parallel shutdown) {
  terminate<policy::parallel>(self, victims)
    .receive([&](atom::done) { /* */ },
             [&](const caf::error& err) { FAIL(err); });
}

TEST(sequential shutdown) {
  terminate<policy::sequential>(self, victims)
    .receive([&](atom::done) { /* */ },
             [&](const caf::error& err) { FAIL(err); });
}

FIXTURE_SCOPE_END()

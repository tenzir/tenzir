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

#define SUITE query_supervisor

#include "vast/system/query_supervisor.hpp"

#include "vast/test/test.hpp"

#include "vast/test/fixtures/actor_system.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/expression.hpp"
#include "vast/ids.hpp"
#include "vast/system/atoms.hpp"

using namespace vast;

namespace {

caf::behavior dummy_evaluator(caf::event_based_actor*, ids x) {
  return {
    [=](const expression&) {
      return x;
    }
  };
}

} // namespace <anonymous>

FIXTURE_SCOPE(query_supervisor_tests, fixtures::deterministic_actor_system)

TEST(lookup) {
  auto e0 = sys.spawn(dummy_evaluator, make_ids({0, 2, 4, 6, 8}));
  auto e1 = sys.spawn(dummy_evaluator, make_ids({1, 7}));
  auto e2 = sys.spawn(dummy_evaluator, make_ids({3, 5}));
  run();
  system::query_map qm{{uuid::random(), {e0, e1}}, {uuid::random(), {e2}}};
  auto coll = sys.spawn(system::query_supervisor, self);
  run();
  expect((caf::atom_value, caf::actor),
         from(coll).to(self).with(system::worker_atom::value, coll));
  self->send(coll, unbox(to<expression>("x == 42")), std::move(qm), self);
  run();
  ids result;
  self->receive_while([&] {
    return result != make_ids({{0, 9}});
  })([&](const ids& x) { result |= x; });
  expect((caf::atom_value, caf::actor),
         from(coll).to(self).with(system::worker_atom::value, coll));
}

FIXTURE_SCOPE_END()

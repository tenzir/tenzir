//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#define SUITE query_supervisor

#include "vast/system/query_supervisor.hpp"

#include "vast/fwd.hpp"

#include "vast/test/fixtures/actor_system.hpp"
#include "vast/test/test.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/expression.hpp"
#include "vast/ids.hpp"
#include "vast/uuid.hpp"

#include <caf/typed_event_based_actor.hpp>

using namespace vast;

// TODO: Bring back.
// namespace {
//
// system::partition_actor::behavior_type
// dummy_partition(system::partition_actor::pointer self, ids x) {
//  return {
//    [=](const vast::expression&, const system::partition_client_actor& client)
//    {
//      self->send(client, x);
//      return atom::done_v;
//    },
//    [=](atom::status, system::status_verbosity) { return caf::settings{}; },
//  };
//}
//
//} // namespace
//
// FIXTURE_SCOPE(query_supervisor_tests, fixtures::deterministic_actor_system)
//
// TEST(lookup) {
//  MESSAGE("spawn supervisor, it should register itself as a worker on
//  launch"); auto sv
//    = sys.spawn(system::query_supervisor,
//                caf::actor_cast<system::query_supervisor_master_actor>(self));
//  run();
//  expect((atom::worker, system::query_supervisor_actor),
//         from(sv).to(self).with(atom::worker_v, sv));
//  MESSAGE("spawn partitions");
//  auto p0 = sys.spawn(dummy_partition, make_ids({0, 2, 4, 6, 8}));
//  auto p1 = sys.spawn(dummy_partition, make_ids({1, 7}));
//  auto p2 = sys.spawn(dummy_partition, make_ids({3, 5}));
//  run();
//  MESSAGE("fill query map and trigger supervisor");
//  system::query_map qm{
//    {uuid::random(), p0}, {uuid::random(), p1}, {uuid::random(), p2}};
//  self->send(sv, unbox(to<expression>("x == 42")), std::move(qm),
//             caf::actor_cast<system::index_client_actor>(self));
//  run();
//  MESSAGE("collect results");
//  bool done = false;
//  ids result;
//  while (!done)
//    self->receive([&](const ids& x) { result |= x; },
//                  [&](atom::done) { done = true; });
//  CHECK_EQUAL(result, make_ids({{0, 9}}));
//  MESSAGE("after completion, the supervisor should register itself again");
//  expect((atom::worker, system::query_supervisor_actor),
//         from(sv).to(self).with(atom::worker_v, sv));
//}
//
// FIXTURE_SCOPE_END()

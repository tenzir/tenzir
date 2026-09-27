// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/importer.hpp"

#include "tenzir/recent_snapshot.hpp"
#include "tenzir/test/test.hpp"

#include <caf/actor_from_state.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/test/fixture/deterministic.hpp>

using namespace tenzir;

TEST("snapshot subscriptions release their terminated receivers") {
  auto f = caf::test::fixture::deterministic{};
  auto resumed = false;
  auto index = f.sys.spawn([&]() -> index_actor::behavior_type {
    return {caf::partial_behavior_init, [](atom::pause, uuid) {},
            [&](atom::resume, uuid) {
              resumed = true;
            },
            [](atom::get, bool) {
              return std::vector<table_slice>{};
            },
            [](atom::get, atom::internal, bool) {
              return std::vector<nova::Events>{};
            }};
  });
  auto importer = f.sys.spawn(caf::actor_from_state<tenzir::importer>, index);
  auto receiver
    = f.sys.spawn([]() -> receiver_actor<nova::Events>::behavior_type {
        return {[](nova::Events) {}};
      });
  auto weak = caf::actor_cast<caf::weak_actor_ptr>(receiver);
  auto ready = false;
  auto client = f.sys.spawn([&](caf::event_based_actor* self) {
    self
      ->mail(atom::get_v, atom::snapshot_v, receiver, false, true, true, false)
      .request(importer, caf::infinite)
      .then(
        [&](NovaRecentSnapshot) {
          ready = true;
        },
        [](caf::error const& error) {
          FAIL("snapshot failed: {}", error);
        });
    return caf::behavior{};
  });
  f.dispatch_messages();
  REQUIRE(ready);
  CHECK(not resumed);
  f.inject_exit(receiver);
  receiver = nullptr;
  f.dispatch_messages();
  CHECK(resumed);
  CHECK(not weak.lock());
  f.inject_exit(importer);
  f.inject_exit(index);
  f.inject_exit(client);
}

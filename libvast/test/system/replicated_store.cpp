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

#include <caf/all.hpp>

#include "vast/system/atoms.hpp"
#include "vast/system/replicated_store.hpp"

#define SUITE consensus
#include "vast/test/test.hpp"
#include "vast/test/fixtures/consensus.hpp"

using namespace caf;
using namespace vast;
using namespace vast::system;

namespace {

constexpr auto timeout = std::chrono::seconds(5);

} // namespace <anonymous>

FIXTURE_SCOPE(consensus_tests, fixtures::consensus)

TEST(single replicated store) {
  MESSAGE("operating with a replicated store");
  auto store = self->spawn(replicated_store<int, int>, server1);
  self->request(store, timeout, put_atom::value, 42, 4711).receive(
    [](ok_atom) { /* nop */ },
    error_handler()
  );
  self->request(store, timeout, put_atom::value, 43, 42).receive(
    [](ok_atom) { /* nop */ },
    error_handler()
  );
  self->request(store, timeout, get_atom::value, 42).receive(
    [&](optional<int> i) {
      REQUIRE(i);
      CHECK_EQUAL(*i, 4711);
    },
    error_handler()
  );
  self->request(store, timeout, add_atom::value, 42, -511).receive(
    [&](int old) {
      CHECK_EQUAL(old, 4711);
    },
    error_handler()
  );
  self->request(store, timeout, get_atom::value, 42).receive(
    [&](optional<int> i) {
      REQUIRE(i);
      CHECK_EQUAL(*i, 4200);
    },
    error_handler()
  );
  self->request(store, timeout, delete_atom::value, 43).receive(
    [](ok_atom) { /* nop */ },
    error_handler()
  );
  self->request(store, timeout, snapshot_atom::value).receive(
    [](ok_atom) { /* nop */ },
    error_handler()
  );
  self->send_exit(store, exit_reason::user_shutdown);
  self->wait_for(store);
  MESSAGE("restarting consensus quorum and store");
  shutdown();
  launch();
  store = self->spawn(replicated_store<int, int>, server1);
  MESSAGE("sleeping until state replay finishes");
  std::this_thread::sleep_for(raft::heartbeat_period * 2);
  MESSAGE("checking value persistence");
  self->request(store, timeout, get_atom::value, 42).receive(
    [&](optional<int> i) {
      REQUIRE(i);
      CHECK_EQUAL(*i, 4200);
    },
    error_handler()
  );
  self->request(store, timeout, get_atom::value, 43).receive(
    [&](optional<int> i) {
      REQUIRE(!i);
    },
    error_handler()
  );
  self->send_exit(store, exit_reason::user_shutdown);
  self->wait_for(store);
}

TEST(multiple replicated stores) {
  auto store1 = self->spawn(replicated_store<int, int>, server1);
  auto store2 = self->spawn(replicated_store<int, int>, server2);
  auto store3 = self->spawn(replicated_store<int, int>, server3);
  self->request(store1, timeout, put_atom::value, 42, 4700).receive(
    [](ok_atom) { /* nop */ },
    error_handler()
  );
  self->request(store2, timeout, add_atom::value, 42, 10).receive(
    [](int) { /* nop */ },
    error_handler()
  );
  self->request(store3, timeout, add_atom::value, 42, 1).receive(
    [](int) { /* nop */ },
    error_handler()
  );
  MESSAGE("sleeping until state replay finishes");
  std::this_thread::sleep_for(raft::heartbeat_period * 2);
  for (auto store : {store1, store2, store3})
    self->request(store, timeout, get_atom::value, 42).receive(
      [&](optional<int> i) {
        REQUIRE(i);
        CHECK_EQUAL(*i, 4711);
      },
      error_handler()
    );
  self->send_exit(store1, exit_reason::user_shutdown);
  self->send_exit(store2, exit_reason::user_shutdown);
  self->send_exit(store3, exit_reason::user_shutdown);
  self->wait_for(store1);
  self->wait_for(store2);
  self->wait_for(store3);
}

FIXTURE_SCOPE_END()

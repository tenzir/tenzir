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
#include "vast/system/raft.hpp"
#include "vast/system/key_value_store.hpp"
#include "vast/system/timeouts.hpp"

#define SUITE consensus
#include "vast/test/test.hpp"
#include "vast/test/fixtures/actor_system.hpp"
#include "vast/test/fixtures/consensus.hpp"

using namespace caf;
using namespace vast;
using namespace vast::system;

FIXTURE_SCOPE(leader_tests, fixtures::actor_system)

TEST_DISABLED(single leader) {
  directory /= "server";
  auto server = self->spawn(raft::consensus, directory);
  self->send(server, id_atom::value, raft::server_id{1});
  self->send(server, run_atom::value);
  self->send(server, subscribe_atom::value, self);
  MESSAGE("sleeping until leader got elected");
  std::this_thread::sleep_for(raft::election_timeout * 2);
  MESSAGE("send two logs to leader");
  auto cmd = make_message(put_atom::value, "foo", 42);
  self->request(server, consensus_timeout, replicate_atom::value, cmd).receive(
    [](ok_atom) { /* nop */ },
    error_handler()
  );
  self->receive(
    [&](raft::index_type i, message) {
      CHECK_EQUAL(i, 2u);
    },
    error_handler()
  );
  cmd = make_message(put_atom::value, "bar", 7);
  self->request(server, consensus_timeout, replicate_atom::value, cmd).receive(
    [](ok_atom) { /* nop */ },
    error_handler()
  );
  self->receive(
    [&](raft::index_type i, message) {
      CHECK_EQUAL(i, 3u);
    },
    error_handler()
  );
  MESSAGE("snapshotting");
  auto last_applied = raft::index_type{3};
  auto state_machine = std::vector<char>(1024);
  self->request(server, consensus_timeout, snapshot_atom::value, last_applied,
                state_machine).receive(
    [&](raft::index_type last_included_index) {
      CHECK_EQUAL(last_included_index, last_applied);
    },
    error_handler()
  );
  MESSAGE("shutting down server");
  self->send_exit(server, exit_reason::user_shutdown);
  self->wait_for(server);
  MESSAGE("respawning");
  server = self->spawn(raft::consensus, directory);
  self->send(server, run_atom::value);
  self->send(server, subscribe_atom::value, self);
  MESSAGE("receiving old state after startup");
  self->receive(
    [&](raft::index_type i, const message& msg) {
      CHECK_EQUAL(i, 3u);
      CHECK_EQUAL(msg.get_as<std::vector<char>>(2).size(), 1024u);
    },
    error_handler()
  );
  MESSAGE("sending another command");
  cmd = make_message(put_atom::value, "baz", 49);
  self->request(server, consensus_timeout, replicate_atom::value, cmd).receive(
    [](ok_atom) { /* nop */ },
    error_handler()
  );
  self->receive(
    [&](raft::index_type i, message) {
      CHECK_EQUAL(i, 5u);
    },
    error_handler()
  );
  MESSAGE("terminating");
  self->send_exit(server, exit_reason::user_shutdown);
  self->wait_for(server);
}

FIXTURE_SCOPE_END()

FIXTURE_SCOPE(consensus_tests, fixtures::consensus)

TEST_DISABLED(basic operations) {
  replicate(server1, make_message("foo"));
  await(2);
}

TEST_DISABLED(manual snapshotting) {
  MESSAGE("replicating commands");
  replicate(server1, make_message("foo"));
  await(1 + 1);
  replicate(server2, make_message("bar"));
  await(2 + 1);
  replicate(server3, make_message("baz"));
  await(3 + 1);
  replicate(server2, make_message("qux"));
  await(4 + 1);
  MESSAGE("sleeping until leader advances commit index");
  std::this_thread::sleep_for(raft::heartbeat_period * 2);
  MESSAGE("performing a manual snapshot at server 1");
  auto state_machine = std::vector<char>(512);
  self->request(server1, consensus_timeout, snapshot_atom::value,
                raft::index_type{3}, state_machine).receive(
    [&](raft::index_type last_included_index) {
      CHECK_EQUAL(last_included_index, 3u);
    },
    error_handler()
  );
  MESSAGE("restarting consensus quorum");
  shutdown();
  launch();
  MESSAGE("consuming initial data");
  // All servers will send us their state. On server #1, we had a snapshot at
  // index 2, which replaces two messages.
  auto last_index = raft::index_type{0};
  auto i = 0;
  self->receive_for(i, 4 * 3 - 1)(
    [&](raft::index_type index, const caf::message& msg)  {
      last_index = index;
      if (self->current_sender() == server1 && index == 2)
        CHECK_EQUAL(msg.get_as<std::vector<char>>(2).size(), 512u);
    },
    error_handler()
  );
  CHECK_EQUAL(last_index, 4u + 1);
  MESSAGE("replicating another command");
  replicate(server3, make_message("foo"));
  await(5 + 2);
}

FIXTURE_SCOPE_END()

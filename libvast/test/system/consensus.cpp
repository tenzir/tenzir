#include <caf/all.hpp>

#include "vast/system/atoms.hpp"
#include "vast/system/consensus.hpp"
#include "vast/system/key_value_store.hpp"

#define SUITE consensus
#include "test.hpp"
#include "fixtures/actor_system.hpp"
#include "fixtures/consensus.hpp"

using namespace caf;
using namespace vast;
using namespace vast::system;

FIXTURE_SCOPE(leader_tests, fixtures::actor_system)

TEST(single leader) {
  directory /= "server";
  auto server = self->spawn(raft::consensus, directory);
  self->send(server, id_atom::value, raft::server_id{1});
  self->send(server, run_atom::value);
  MESSAGE("send two logs to leader");
  auto cmd = make_message(put_atom::value, "foo", 42);
  MESSAGE("sleeping until leader got elected");
  std::this_thread::sleep_for(raft::election_timeout * 2);
  MESSAGE("send two logs to leader");
  auto timeout = std::chrono::seconds(3);
  self->request(server, timeout, replicate_atom::value, cmd).receive(
    [&](ok_atom, raft::index_type i) {
      CHECK_EQUAL(i, 1u);
    },
    error_handler()
  );
  cmd = make_message(put_atom::value, "bar", 7);
  self->request(server, timeout, replicate_atom::value, cmd).receive(
    [&](ok_atom, raft::index_type i) {
      CHECK_EQUAL(i, 2u);
    },
    error_handler()
  );
  MESSAGE("snapshotting");
  self->request(server, timeout, snapshot_atom::value).receive(
    [&](raft::index_type last_included_index) {
      CHECK_EQUAL(last_included_index, 2u);
    },
    error_handler()
  );
  MESSAGE("shutting down server");
  self->send_exit(server, exit_reason::user_shutdown);
  self->wait_for(server);
  MESSAGE("sending another command");
  server = self->spawn(raft::consensus, directory);
  self->send(server, run_atom::value);
  cmd = make_message(put_atom::value, "baz", 49);
  self->request(server, timeout, replicate_atom::value, cmd).receive(
    [&](ok_atom, raft::index_type i) {
      CHECK_EQUAL(i, 3u);
    },
    error_handler()
  );
  MESSAGE("terminating");
  self->send_exit(server, exit_reason::user_shutdown);
}

FIXTURE_SCOPE_END()

FIXTURE_SCOPE(consensus_tests, fixtures::consensus)

TEST(basic operations) {
  auto cmd = make_message(put_atom::value, "foo", 42);
  self->request(server1, timeout, replicate_atom::value, cmd).receive(
    [&](ok_atom, raft::index_type i) {
      CHECK_EQUAL(i, 1u);
    },
    error_handler()
  );
  shutdown();
}

TEST(manual snapshotting) {
  MESSAGE("adding commands");
  auto index = raft::index_type{0};
  auto add = [&](auto server, auto cmd) {
    self->request(server, timeout, replicate_atom::value, cmd).receive(
      [&](ok_atom, raft::index_type i) {
        CHECK_EQUAL(i, ++index);
      },
      error_handler()
    );
  };
  add(server1, make_message(put_atom::value, "foo", 42));
  add(server2, make_message(put_atom::value, "bar", -42));
  add(server1, make_message(put_atom::value, "baz", 4711));
  MESSAGE("sleeping until leader advances commit index to 3");
  std::this_thread::sleep_for(raft::heartbeat_period * 2);
  MESSAGE("performing a manual snapshot at server 1");
  self->request(server1, timeout, snapshot_atom::value).receive(
    [&](raft::index_type last_included_index) {
      CHECK_EQUAL(last_included_index, 3u);
    },
    error_handler()
  );
  shutdown();
  MESSAGE("bringing cluster back up");
  launch();
  std::this_thread::sleep_for(raft::heartbeat_period * 2);
  MESSAGE("adding another command");
  add(server3, make_message(put_atom::value, "qux", 99));
  add(server2, make_message(put_atom::value, "corge", -99));
  shutdown();
}

FIXTURE_SCOPE_END()

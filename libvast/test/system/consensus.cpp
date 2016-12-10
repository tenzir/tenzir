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
  auto timeout = std::chrono::seconds(3);
  auto config = raft::configuration{{"id", "1"}};
  auto consensus = self->spawn(raft::consensus, config);
  self->send(consensus, leader_atom::value); // manual election
  MESSAGE("send two logs to leader");
  auto cmd = make_message(put_atom::value, "foo", 42);
  self->request(consensus, timeout, replicate_atom::value, cmd).receive(
    [&](ok_atom, raft::index_type i) {
      CHECK_EQUAL(i, 1u);
    },
    error_handler()
  );
  cmd = make_message(put_atom::value, "bar", 7);
  self->request(consensus, timeout, replicate_atom::value, cmd).receive(
    [&](ok_atom, raft::index_type i) {
      CHECK_EQUAL(i, 2u);
    },
    error_handler()
  );
  self->send(consensus, shutdown_atom::value);
}

FIXTURE_SCOPE_END()

FIXTURE_SCOPE(consensus_tests, fixtures::consensus)

TEST(basic operations) {
  MESSAGE("performing election");
  self->send(server1, leader_atom::value);
  MESSAGE("submitting command change through leader");
  auto cmd = make_message(put_atom::value, "foo", 42);
  self->request(server1, timeout, replicate_atom::value, cmd).receive(
    [&](ok_atom, raft::index_type i) {
      CHECK_EQUAL(i, 1u);
    },
    error_handler()
  );
  std::this_thread::sleep_for(std::chrono::seconds(1));
  shutdown();
}

FIXTURE_SCOPE_END()

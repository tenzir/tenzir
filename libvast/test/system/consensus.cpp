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
  auto config = raft::configuration{{"id", "1"}};
  auto consensus = self->spawn(raft::consensus, config);
  MESSAGE("send two logs to leader");
  auto cmd = make_message(put_atom::value, "foo", 42);
  MESSAGE("sleeping until leader got elected");
  std::this_thread::sleep_for(raft::election_timeout * 2);
  auto timeout = std::chrono::seconds(3);
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
  MESSAGE("submitting command change through leader");
  auto cmd = make_message(put_atom::value, "foo", 42);
  self->request(server1, timeout, replicate_atom::value, cmd).receive(
    [&](ok_atom, raft::index_type i) {
      CHECK_EQUAL(i, 1u);
    },
    error_handler()
  );
  shutdown();
}

FIXTURE_SCOPE_END()

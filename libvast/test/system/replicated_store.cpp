#include <caf/all.hpp>

#include "vast/system/atoms.hpp"
#include "vast/system/replicated_store.hpp"

#define SUITE consensus
#include "test.hpp"
#include "fixtures/consensus.hpp"

using namespace caf;
using namespace vast;
using namespace vast::system;

namespace {

constexpr auto timeout = std::chrono::seconds(5);

} // namespace <anonymous>

FIXTURE_SCOPE(consensus_tests, fixtures::consensus)

TEST(replicated store) {
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

FIXTURE_SCOPE_END()

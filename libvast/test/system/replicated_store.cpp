#include <caf/all.hpp>

#include "vast/system/atoms.hpp"
#include "vast/system/replicated_store.hpp"

#define SUITE consensus
#include "test.hpp"
#include "fixtures/consensus.hpp"

using namespace caf;
using namespace vast;
using namespace vast::system;

FIXTURE_SCOPE(consensus_tests, fixtures::consensus)

TEST(replicated store) {
  MESSAGE("operating with a replicated store");
  auto store = self->spawn(replicated_store<int, int>, server1, timeout);
  self->request(store, timeout, put_atom::value, 42, 4711).receive(
    [&](ok_atom) { /* nop */ },
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
    [&](ok_atom) { /* nop */ },
    error_handler()
  );
  self->request(store, timeout, get_atom::value, 42).receive(
    [&](optional<int> i) {
      REQUIRE(i);
      CHECK_EQUAL(*i, 4200);
    },
    error_handler()
  );
  shutdown();
}

FIXTURE_SCOPE_END()

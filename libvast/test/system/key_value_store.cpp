#include <caf/all.hpp>

#include "vast/system/atoms.hpp"
#include "vast/system/key_value_store.hpp"

#define SUITE system
#include "test.hpp"
#include "fixtures/actor_system.hpp"

using namespace caf;
using namespace vast;
using namespace vast::system;

FIXTURE_SCOPE(key_value_store_tests, fixtures::actor_system)

TEST(key-value store) {
  auto store = self->spawn(data_store<std::string, int>);
  MESSAGE("put two values");
  self->request(store, infinite, put_atom::value, "foo", 42).receive(
    [&](ok_atom) {},
    error_handler()
  );
  MESSAGE("get a key with a single value");
  self->request(store, infinite, get_atom::value, "foo").receive(
    [&](optional<int> result) { 
      REQUIRE(result);
      CHECK_EQUAL(*result, 42);
    },
    error_handler()
  );
  MESSAGE("get an invalid key value");
  self->request(store, infinite, get_atom::value, "bar").receive(
    [&](optional<int> result) {
      CHECK(!result);
    },
    error_handler()
  );
  //MESSAGE("add to an existing single value");
  //self->sync_send(store, add_atom::value, key::str("foo", "bar"), 1).await(
  //  [&](int old, int now) {
  //    CHECK(old == 42);
  //    CHECK(now == 43);
  //  }
  //);
  //MESSAGE("add to an non-existing single value");
  //self->sync_send(store, add_atom::value, key::str("foo", "qux"), 10).await(
  //  [&](int old, int now) {
  //    CHECK(old == 0);
  //    CHECK(now == 10);
  //  }
  //);
  //MESSAGE("delete a key");
  //self->sync_send(store, delete_atom::value, key::str("foo", "bar")).await(
  //  [&](uint64_t n) { CHECK(n == 1); }
  //);
  //self->sync_send(store, exists_atom::value, key::str("foo", "bar")).await(
  //  [&](bool b) { CHECK(! b); }
  //);
  //MESSAGE("put/get an empty value");
  //self->sync_send(store, put_atom::value, "meow").await([&](ok_atom) {});
  //self->sync_send(store, get_atom::value, "meow").await(others >> [&] {});
  //self->send_exit(store, exit::done);
}

//TEST(key-value store (distributed consensus)) {
//  scoped_actor self;
//  self->on_sync_failure([&] {
//    FAIL("got unexpected message: " << to_string(self->current_message()));
//  });
//  // In the future, we add an election phase.
//  MESSAGE("setup leader-follower relationship");
//  auto leader = self->spawn(key_value_store::make, path{});
//  auto follower1 = self->spawn(key_value_store::make, path{});
//  auto follower2 = self->spawn(key_value_store::make, path{});
//  self->send(leader, leader_atom::value);
//  self->send(leader, follower_atom::value, add_atom::value, follower1);
//  self->send(leader, follower_atom::value, add_atom::value, follower2);
//  MESSAGE("put value");
//  self->sync_send(leader, put_atom::value, "foo", 42).await([](ok_atom) {});
//  MESSAGE("get value from follower");
//  self->sync_send(follower1, get_atom::value, "foo").await(
//    [&](int value) { CHECK(value == 42); }
//  );
//  MESSAGE("insert value in follower A and get it from follower B");
//  self->sync_send(follower1, put_atom::value, "bar", 84).await([](ok_atom) {});
//  self->sync_send(follower2, get_atom::value, "bar").await(
//    [&](int value) { CHECK(value == 84); }
//  );
//  self->send_exit(follower1, exit::done);
//  self->send_exit(follower2, exit::done);
//  self->send_exit(leader, exit::done);
//  self->await_all_other_actors_done();
//}

FIXTURE_SCOPE_END()

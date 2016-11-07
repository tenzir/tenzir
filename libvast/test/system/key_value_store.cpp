#include "vast/key.hpp"
#include "vast/none.hpp"
#include "vast/actor/atoms.hpp"
#include "vast/actor/key_value_store.hpp"

#define SUITE actors
#include "test.hpp"

using namespace vast;

TEST(key-value store (leader interface)) {
  scoped_actor self;
  self->on_sync_failure([&] {
    FAIL("got unexpected message: " << to_string(self->current_message()));
  });
  auto s = self->spawn(key_value_store::make, path{});
  self->send(s, leader_atom::value);
  MESSAGE("put two values");
  self->sync_send(s, put_atom::value, "foo.bar", 42).await([&](ok_atom) {});
  self->sync_send(s, put_atom::value, "foo.baz", 84).await([&](ok_atom) {});
  MESSAGE("get a key with a single value");
  self->sync_send(s, get_atom::value, key::str("foo", "bar")).await(
    [&](int value) { CHECK(value == 42); }
  );
  MESSAGE("get an invalid key value");
  self->sync_send(s, get_atom::value, "foo.corge").await([&](vast::none) {});
  MESSAGE("get multiple values");
  self->sync_send(s, list_atom::value, "foo").await(
    [&](std::map<std::string, message> const& map) {
      REQUIRE(map.size() == 2);
      CHECK(map.begin()->first == key::str("foo", "bar"));
      CHECK(map.begin()->second.get_as<int>(0) == 42);
      CHECK(map.rbegin()->first == key::str("foo", "baz"));
      CHECK(map.rbegin()->second.get_as<int>(0) == 84);
    }
  );
  MESSAGE("add to an existing single value");
  self->sync_send(s, add_atom::value, key::str("foo", "bar"), 1).await(
    [&](int old, int now) {
      CHECK(old == 42);
      CHECK(now == 43);
    }
  );
  MESSAGE("add to an non-existing single value");
  self->sync_send(s, add_atom::value, key::str("foo", "qux"), 10).await(
    [&](int old, int now) {
      CHECK(old == 0);
      CHECK(now == 10);
    }
  );
  MESSAGE("delete a key");
  self->sync_send(s, delete_atom::value, key::str("foo", "bar")).await(
    [&](uint64_t n) { CHECK(n == 1); }
  );
  self->sync_send(s, exists_atom::value, key::str("foo", "bar")).await(
    [&](bool b) { CHECK(! b); }
  );
  MESSAGE("put/get an empty value");
  self->sync_send(s, put_atom::value, "meow").await([&](ok_atom) {});
  self->sync_send(s, get_atom::value, "meow").await(others >> [&] {});
  self->send_exit(s, exit::done);
  self->await_all_other_actors_done();
}

TEST(key-value store (distributed consensus)) {
  scoped_actor self;
  self->on_sync_failure([&] {
    FAIL("got unexpected message: " << to_string(self->current_message()));
  });
  // In the future, we add an election phase.
  MESSAGE("setup leader-follower relationship");
  auto leader = self->spawn(key_value_store::make, path{});
  auto follower1 = self->spawn(key_value_store::make, path{});
  auto follower2 = self->spawn(key_value_store::make, path{});
  self->send(leader, leader_atom::value);
  self->send(leader, follower_atom::value, add_atom::value, follower1);
  self->send(leader, follower_atom::value, add_atom::value, follower2);
  MESSAGE("put value");
  self->sync_send(leader, put_atom::value, "foo", 42).await([](ok_atom) {});
  MESSAGE("get value from follower");
  self->sync_send(follower1, get_atom::value, "foo").await(
    [&](int value) { CHECK(value == 42); }
  );
  MESSAGE("insert value in follower A and get it from follower B");
  self->sync_send(follower1, put_atom::value, "bar", 84).await([](ok_atom) {});
  self->sync_send(follower2, get_atom::value, "bar").await(
    [&](int value) { CHECK(value == 84); }
  );
  self->send_exit(follower1, exit::done);
  self->send_exit(follower2, exit::done);
  self->send_exit(leader, exit::done);
  self->await_all_other_actors_done();
}

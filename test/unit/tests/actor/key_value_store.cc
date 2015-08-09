#include <caf/all.hpp>

#include "vast/none.h"
#include "vast/actor/key_value_store.h"

#define SUITE actors
#include "test.h"

using namespace caf;
using namespace vast;

TEST(key-value store)
{
  scoped_actor self;
  self->on_sync_failure([&] {
    FAIL("got unexpected message: " << to_string(self->current_message()));
  });
  auto s = self->spawn<key_value_store>();
  MESSAGE("put two values");
  self->sync_send(s, put_atom::value, "/foo/bar", uint8_t{42}).await(
    [&](ok_atom) { /* nop */ }
  );
  self->sync_send(s, put_atom::value, "/foo/baz", uint8_t{84}).await(
    [&](ok_atom) { /* nop */ }
  );
  MESSAGE("get a key with a single value");
  self->sync_send(s, get_atom::value, "/foo/bar").await(
    [&](uint8_t value) { CHECK(value == 42); }
  );
  MESSAGE("get an invalid key value");
  self->sync_send(s, get_atom::value, "/foo/corge").await(
    [&](vast::none) { REQUIRE(true); }
  );
  MESSAGE("get multiple values");
  self->sync_send(s, list_atom::value, "/foo").await(
    [&](std::map<std::string, message> const& map)
    {
      REQUIRE(map.size() == 2);
      CHECK(map.begin()->first == "/foo/bar");
      CHECK(map.begin()->second.get_as<uint8_t>(0) == 42);
      CHECK(map.rbegin()->first == "/foo/baz");
      CHECK(map.rbegin()->second.get_as<uint8_t>(0) == 84);
    }
  );
  MESSAGE("delete a key");
  self->sync_send(s, delete_atom::value, "/foo/bar").await(
    [&](uint64_t n) { CHECK(n == 1); }
  );
  self->sync_send(s, exists_atom::value, "/foo/bar").await(
    [&](bool b) { CHECK(! b); }
  );
  MESSAGE("delete a value");
  self->sync_send(s, put_atom::value, "/foo/qux", "quuuux").await(
    [&](ok_atom) { /* nop */ }
  );
  self->sync_send(s, delete_atom::value, "/foo", uint8_t{84}).await(
    [&](uint64_t n) { CHECK(n == 1); }
  );
  self->sync_send(s, exists_atom::value, "/foo/baz").await(
    [&](bool b) { CHECK(! b); }
  );
  MESSAGE("put/get an empty value");
  self->sync_send(s, put_atom::value, "meow").await(
    [&](ok_atom) { /* nop */ }
  );
  self->sync_send(s, get_atom::value, "meow").await(
    others >> [&] { /* all good */ }
  );
  self->send_exit(s, exit::done);
  self->await_all_other_actors_done();
}

TEST(key-value store wrapper)
{
  scoped_actor self;
  auto s = self->spawn<key_value_store>();
  auto w = key_value_store::wrapper{s};
  MESSAGE("put two values");
  CHECK(w.put("/foo/bar", uint8_t{42}));
  CHECK(w.put("/foo/baz", uint8_t{84}));
  MESSAGE("get values value");
  w.get("/foo/bar").apply([](uint8_t value) { CHECK(value == 42); });
  w.get("/foo/corge").apply([](vast::none) { REQUIRE(true); });
  MESSAGE("get multiple values");
  auto map = w.list("/foo");
  REQUIRE(map.size() == 2);
  CHECK(map.begin()->first == "/foo/bar");
  CHECK(map.begin()->second.get_as<uint8_t>(0) == 42);
  CHECK(map.rbegin()->first == "/foo/baz");
  CHECK(map.rbegin()->second.get_as<uint8_t>(0) == 84);
  MESSAGE("delete a key");
  CHECK(w.erase("/foo/bar") == 1);
  CHECK(! w.exists("/foo/bar"));
  MESSAGE("put/get an empty value");
  CHECK(w.put("meow"));
  self->send_exit(s, exit::done);
  self->await_all_other_actors_done();
}

TEST(distributed key-value store)
{
  scoped_actor self;
  self->on_sync_failure([&] {
    FAIL("got unexpected message: " << to_string(self->current_message()));
  });
  auto s1 = self->spawn<key_value_store>();
  auto s2 = self->spawn<key_value_store>();
  MESSAGE("setup peeering");
  self->sync_send(s1, peer_atom::value, s2).await(
    [](ok_atom) { /* nop */ }
  );
  self->sync_send(s1, put_atom::value, "foo", 42).await([](ok_atom) {});
  // FIXME: we currently have no synchronous way to ensure that all data is
  // commit globally, which is why we have to wait in the form of sleeping
  // until the data is available at all peers.
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  MESSAGE("get value from peer");
  self->sync_send(s2, get_atom::value, "foo").await(
    [&](int value) { CHECK(value == 42); }
  );
  MESSAGE("insert value in peer and get it from other");
  self->sync_send(s2, put_atom::value, "bar", 84).await([](ok_atom) {});
  // Give some time to propagate the value to the peer.
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  self->sync_send(s1, get_atom::value, "bar").await(
    [&](int value) { CHECK(value == 84); }
  );
  self->send_exit(s1, exit::done);
  self->send_exit(s2, exit::done);
  self->await_all_other_actors_done();
}

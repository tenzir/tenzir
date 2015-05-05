#include <caf/all.hpp>

#include "vast/none.h"
#include "vast/actor/key_value_store.h"

#include "framework/unit.h"

using namespace caf;
using namespace vast;

SUITE("core")

TEST("key-value store")
{
  scoped_actor self;
  auto s = self->spawn<key_value_store>();
  VAST_INFO("put two values");
  self->sync_send(s, put_atom::value, "/foo/bar", 42).await(
    [&](ok_atom) { /* nop */ }
  );
  self->sync_send(s, put_atom::value, "/foo/baz", 84).await(
    [&](ok_atom) { /* nop */ }
  );
  VAST_INFO("get a key with a single value");
  self->sync_send(s, get_atom::value, "/foo/bar").await(
    [&](int value) { CHECK(value == 42); },
    others() >> [&] { REQUIRE(false); }
  );
  VAST_INFO("get an invalid key value");
  self->sync_send(s, get_atom::value, "/foo/corge").await(
    [&](vast::none) { REQUIRE(true); },
    others() >> [&] { REQUIRE(false); }
  );
  VAST_INFO("get multiple values");
  self->sync_send(s, list_atom::value, "/foo").await(
    [&](std::map<std::string, message> const& map)
    {
      CHECK(map.begin()->first == "/foo/bar");
      CHECK(map.begin()->second.get_as<int>(0) == 42);
      CHECK(map.rbegin()->first == "/foo/baz");
      CHECK(map.rbegin()->second.get_as<int>(0) == 84);
    },
    others() >> [&] { REQUIRE(false); }
  );
  VAST_INFO("delete a key");
  self->sync_send(s, delete_atom::value, "/foo/bar").await(
    [&](uint64_t n) { CHECK(n == 1); }
  );
  self->sync_send(s, exists_atom::value, "/foo/bar").await(
    [&](bool b) { CHECK(! b); }
  );
  VAST_INFO("delete a value");
  self->sync_send(s, put_atom::value, "/foo/qux", "quuuux").await(
    [&](ok_atom) { /* nop */ }
  );
  self->sync_send(s, delete_atom::value, "/foo", 84).await(
    [&](uint64_t n) { CHECK(n == 1); }
  );
  self->sync_send(s, exists_atom::value, "/foo/baz").await(
    [&](bool b) { CHECK(! b); }
  );
}

TEST("key-value store (distributed)")
{
  scoped_actor self;
  auto s1 = self->spawn<key_value_store>();
  auto s2 = self->spawn<key_value_store>();
  VAST_INFO("setup peeering");
  self->sync_send(s1, peer_atom::value, s2).await(
    [](ok_atom) { /* nop */ }
  );
  self->sync_send(s1, put_atom::value, "foo", 42).await([](ok_atom) {});
  // FIXME: we currently have no synchronous way to ensure that all data is
  // commit globally, which is why we have to wait in the form of sleeping
  // until the data is available at all peers.
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  VAST_INFO("get value from peer");
  self->sync_send(s2, get_atom::value, "foo").await(
    [&](int value) { CHECK(value == 42); },
    others() >> [&] { REQUIRE(false); }
  );
  VAST_INFO("insert value in peer and get it from other");
  self->sync_send(s2, put_atom::value, "bar", 84).await([](ok_atom) {});
  // Give some time to propagate the value to the peer.
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  self->sync_send(s1, get_atom::value, "bar").await(
    [&](int value) { CHECK(value == 84); },
    others() >> [&] { REQUIRE(false); }
  );
}

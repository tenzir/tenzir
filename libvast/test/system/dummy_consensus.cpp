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

#include "vast/system/dummy_consensus.hpp"

#include "vast/fwd.hpp"

#include <caf/all.hpp>

#define SUITE dummy_consensus
#include "vast/test/test.hpp"
#include "vast/test/fixtures/actor_system.hpp"

using namespace caf;
using namespace vast;
using namespace vast::system;

FIXTURE_SCOPE(dummy_consensus_tests, fixtures::actor_system)

TEST(dummy consensus) {
  auto store_dir = directory / "dummy-consensus";
  {
    auto store = self->spawn(dummy_consensus, store_dir);
    MESSAGE("put two values");
    self->request(store, infinite, atom::put::value, "foo", data{42})
      .receive([&](atom::ok) {}, error_handler());
    MESSAGE("get a key with a single value");
    self->request(store, infinite, atom::get::value, "foo")
      .receive(
        [&](optional<data> result) {
          REQUIRE(result);
          CHECK_EQUAL(*result, data{42});
        },
        error_handler());
    MESSAGE("get an invalid key value");
    self->request(store, infinite, atom::get::value, "bar")
      .receive([&](optional<data> result) { CHECK(!result); }, error_handler());
    MESSAGE("add to an existing single value");
    self->request(store, infinite, atom::add::value, "foo", data{1})
      .receive([&](data old) { CHECK_EQUAL(old, data{42}); }, error_handler());
    MESSAGE("add to a non-existing single value");
    self->request(store, infinite, atom::add::value, "baz", data{1})
      .receive([&](data old) { CHECK_EQUAL(old, caf::none); }, error_handler());
    MESSAGE("delete a key");
    self->request(store, infinite, atom::erase::value, "foo")
      .receive([](atom::ok) { /* nop */ }, error_handler());
    MESSAGE("restart the store, forcing a serialize -> deserialize loop");
    self->send_exit(store, exit_reason::user_shutdown);
  }
  {
    auto store = self->spawn(dummy_consensus, store_dir);
    MESSAGE("get a value from the store's previous lifetime");
    self->request(store, infinite, atom::get::value, "baz")
      .receive(
        [&](optional<data> result) {
          REQUIRE(result);
          CHECK_EQUAL(*result, data{1});
        },
        error_handler());
    MESSAGE("get a key that was deleted during the store's previous lfetime");
    self->request(store, infinite, atom::get::value, "foo")
      .receive([&](optional<data> result) { CHECK(!result); }, error_handler());
  }
}

FIXTURE_SCOPE_END()

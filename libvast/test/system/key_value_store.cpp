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

#include "vast/fwd.hpp"
#include "vast/system/data_store.hpp"

#include <caf/all.hpp>

#define SUITE system
#include "vast/test/test.hpp"
#include "vast/test/fixtures/actor_system.hpp"

using namespace caf;
using namespace vast;
using namespace vast::system;

FIXTURE_SCOPE(key_value_store_tests, fixtures::actor_system)

TEST(key-value store) {
  auto store = self->spawn(data_store<std::string, int>);
  MESSAGE("put two values");
  self->request(store, infinite, atom::put_v, "foo", 42)
    .receive([&](atom::ok) {}, error_handler());
  MESSAGE("get a key with a single value");
  self->request(store, infinite, atom::get_v, "foo")
    .receive(
      [&](optional<int> result) {
        REQUIRE(result);
        CHECK_EQUAL(*result, 42);
      },
      error_handler());
  MESSAGE("get an invalid key value");
  self->request(store, infinite, atom::get_v, "bar")
    .receive([&](optional<int> result) { CHECK(!result); }, error_handler());
  MESSAGE("add to an existing single value");
  self->request(store, infinite, atom::add_v, "foo", 1)
    .receive([&](int old) { CHECK_EQUAL(old, 42); }, error_handler());
  MESSAGE("add to a non-existing single value");
  self->request(store, infinite, atom::add_v, "baz", 1)
    .receive([&](int old) { CHECK_EQUAL(old, 0); }, error_handler());
  MESSAGE("delete a key");
  self->request(store, infinite, atom::erase_v, "foo")
    .receive([](atom::ok) { /* nop */ }, error_handler());
}

FIXTURE_SCOPE_END()

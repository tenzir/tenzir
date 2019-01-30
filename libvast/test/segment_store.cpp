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

#define SUITE segment_store

#include "vast/segment_store.hpp"

#include "vast/test/test.hpp"
#include "vast/test/fixtures/events.hpp"
#include "vast/test/fixtures/filesystem.hpp"

#include "vast/ids.hpp"
#include "vast/si_literals.hpp"
#include "vast/table_slice.hpp"

using namespace vast;
using namespace binary_byte_literals;

struct fixture : fixtures::events, fixtures::filesystem {};

FIXTURE_SCOPE(segment_store_tests, fixture)

TEST(construction and querying) {
  auto path = directory / "segments";
  auto store = segment_store::make(path, 512_KiB, 2);
  REQUIRE(store);
  for (auto& slice : zeek_conn_log_slices)
    REQUIRE(!store->put(slice));
  auto slices = store->get(make_ids({0, 6, 19, 21}));
  REQUIRE(slices);
  REQUIRE_EQUAL(slices->size(), 2u);
}

TEST(sessionized extraction) {
  auto path = directory / "segments";
  auto store = segment_store::make(path, 512_KiB, 2);
  REQUIRE(store);
  for (auto& slice : zeek_conn_log_slices)
    REQUIRE(!store->put(slice));
  auto session = store->extract(make_ids({0, 6, 19, 21}));
  auto slice0 = session->next();
  REQUIRE(slice0);
  REQUIRE_EQUAL((*slice0)->offset(), 0u);
  auto slice1 = session->next();
  REQUIRE(slice1);
  REQUIRE_EQUAL((*slice1)->offset(), 16u);
  auto slice2 = session->next();
  REQUIRE(!slice2);
  REQUIRE(slice2.error() == caf::no_error);
}

FIXTURE_SCOPE_END()

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

#include "test.hpp"
#include "test/fixtures/actor_system_and_events.hpp"

#include "vast/const_table_slice_handle.hpp"
#include "vast/ids.hpp"
#include "vast/si_literals.hpp"
#include "vast/table_slice.hpp"

using namespace vast;
using namespace binary_byte_literals;

FIXTURE_SCOPE(segment_store_tests,
              fixtures::deterministic_actor_system_and_events)

TEST(construction and querying) {
  // FIXME: use directory from fixture
  rm("foo");
  auto store = segment_store::make(sys, path{"foo"}, 512_KiB, 2);
  REQUIRE(store);
  for (auto& slice : const_bro_conn_log_slices)
    REQUIRE(!store->put(slice));
  auto slices = store->get(make_ids({42, 1337, 8401}));
  REQUIRE(slices);
  REQUIRE_EQUAL(slices->size(), 3u);
}

FIXTURE_SCOPE_END()

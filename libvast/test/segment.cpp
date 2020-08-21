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

#define SUITE segment

#include "vast/segment.hpp"

#include "vast/test/fixtures/events.hpp"
#include "vast/test/test.hpp"

#include "vast/ids.hpp"
#include "vast/load.hpp"
#include "vast/save.hpp"
#include "vast/segment_builder.hpp"
#include "vast/table_slice.hpp"

#include <caf/test/dsl.hpp>

using namespace vast;

FIXTURE_SCOPE(segment_tests, fixtures::events)

TEST(construction and querying) {
  segment_builder builder;
  for (auto& slice : zeek_conn_log)
    if (auto err = builder.add(slice))
      FAIL(err);
  auto x = builder.finish();
  CHECK_EQUAL(x.num_slices(), zeek_conn_log.size());
  MESSAGE("lookup IDs for some segments");
  auto slices = unbox(x.lookup(make_ids({0, 6, 19, 21})));
  REQUIRE_EQUAL(slices.size(), 2u); // [0,8), [16,24)
  CHECK_EQUAL(*slices[0], *zeek_conn_log[0]);
  CHECK_EQUAL(*slices[1], *zeek_conn_log[2]);
}

TEST(serialization) {
  segment_builder builder;
  auto slice = zeek_conn_log[0];
  REQUIRE(!builder.add(slice));
  auto x = builder.finish();
  chunk_ptr chk;
  std::vector<char> buf;
  REQUIRE_EQUAL(save(nullptr, buf, x.chunk()), caf::none);
  REQUIRE_EQUAL(load(nullptr, buf, chk), caf::none);
  REQUIRE_NOT_EQUAL(chk, nullptr);
  auto y = segment::make(chk);
  REQUIRE(y);
  CHECK_EQUAL(x.ids(), y->ids());
  CHECK_EQUAL(x.num_slices(), y->num_slices());
}

FIXTURE_SCOPE_END()

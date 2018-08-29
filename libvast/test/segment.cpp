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
#include "vast/segment_builder.hpp"

#include "test.hpp"
#include "test/fixtures/actor_system_and_events.hpp"

#include <caf/binary_deserializer.hpp>
#include <caf/binary_serializer.hpp>

#include "vast/ids.hpp"
#include "vast/const_table_slice_handle.hpp"
#include "vast/load.hpp"
#include "vast/table_slice.hpp"
#include "vast/save.hpp"

using namespace vast;

FIXTURE_SCOPE(segment_tests, fixtures::deterministic_actor_system_and_events)

TEST(construction and querying) {
  segment_builder builder{sys};
  for (auto& slice : const_bro_conn_log_slices)
    REQUIRE(!builder.add(slice));
  auto segment = builder.finish();
  REQUIRE(segment);
  auto x = *segment;
  CHECK_EQUAL(x->num_slices(), const_bro_conn_log_slices.size());
  MESSAGE("lookup IDs for some segments");
  auto xs = x->lookup(make_ids({0, 42, 1337, 4711}));
  REQUIRE(xs);
  auto& slices = *xs;
  REQUIRE_EQUAL(slices.size(), 3u); // [0,100), [1300,1400), [4700,4800)
  CHECK_EQUAL(*slices[0], *const_bro_conn_log_slices[0]);
  CHECK_EQUAL(*slices[1], *const_bro_conn_log_slices[13]);
  CHECK_EQUAL(*slices[2], *const_bro_conn_log_slices[47]);
}

TEST(serialization) {
  segment_builder builder{sys};
  auto slice = const_bro_conn_log_slices[0];
  REQUIRE(!builder.add(slice));
  auto segment = builder.finish();
  REQUIRE(segment);
  auto x = *segment;
  std::vector<char> buf;
  caf::binary_serializer sink{sys, buf};
  auto error = sink(x);
  REQUIRE(!error);
  CHECK(buf.size() > 0);
  segment_ptr y;
  caf::binary_deserializer source{sys, buf};
  error = source(y);
  REQUIRE(!error);
  REQUIRE(y);
  CHECK_EQUAL(y->num_slices(), 1u);
  CHECK(std::equal(x->chunk()->begin(), x->chunk()->end(),
                   y->chunk()->begin(), y->chunk()->end()));
}

FIXTURE_SCOPE_END()

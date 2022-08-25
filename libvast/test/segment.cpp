//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#define SUITE segment

#include "vast/segment.hpp"

#include "vast/detail/legacy_deserialize.hpp"
#include "vast/detail/serialize.hpp"
#include "vast/ids.hpp"
#include "vast/segment_builder.hpp"
#include "vast/table_slice.hpp"
#include "vast/test/fixtures/events.hpp"
#include "vast/test/test.hpp"

#include <caf/test/dsl.hpp>

using namespace vast;

FIXTURE_SCOPE(segment_tests, fixtures::events)

TEST(construction and querying) {
  segment_builder builder{1024};
  for (auto& slice : zeek_conn_log)
    if (auto err = builder.add(slice))
      FAIL(err);
  auto x = builder.finish();
  CHECK_EQUAL(x.num_slices(), zeek_conn_log.size());
  MESSAGE("lookup IDs for some segments");
  auto slices = unbox(x.lookup(make_ids({0, 6, 19, 21})));
  REQUIRE_EQUAL(slices.size(), 2u); // [0,8), [16,24)
  CHECK_EQUAL(slices[0], zeek_conn_log[0]);
  CHECK_EQUAL(slices[1], zeek_conn_log[2]);
  auto y = segment::copy_without(x, make_ids({19, 21}));
  REQUIRE_NOERROR(y);
  auto slices2 = unbox(y->lookup(make_ids({0, 6, 19, 21})));
  REQUIRE_EQUAL(slices2.size(), 1u); // [0,8)
  CHECK_EQUAL(slices2[0], zeek_conn_log[0]);
}

TEST(serialization) {
  segment_builder builder{1024};
  auto slice = zeek_conn_log[0];
  REQUIRE(!builder.add(slice));
  auto x = builder.finish();
  chunk_ptr chk;
  caf::byte_buffer buf;
  REQUIRE_EQUAL(detail::serialize(buf, x.chunk()), true);
  REQUIRE_EQUAL(detail::legacy_deserialize(buf, chk), true);
  REQUIRE_NOT_EQUAL(chk, nullptr);
  auto y = segment::make(std::move(chk));
  REQUIRE(y);
  CHECK_EQUAL(x.ids(), y->ids());
  CHECK_EQUAL(x.num_slices(), y->num_slices());
}

FIXTURE_SCOPE_END()

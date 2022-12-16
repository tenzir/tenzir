//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#define SUITE segment

#include "vast/segment.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/data.hpp"
#include "vast/io/read.hpp"
#include "vast/test/test.hpp"

#include <caf/test/dsl.hpp>

namespace vast {

TEST(segment store queries) {
  auto bytes = unbox(vast::io::read(VAST_TEST_PATH "artifacts/segment_stores/"
                                                   "zeek.conn.store"));
  auto chunk = chunk::make(std::move(bytes));
  auto segment = unbox(segment::make(std::move(chunk)));
  CHECK_EQUAL(segment.num_slices(), 1u);
  auto slices = unbox(segment.lookup(make_ids({0, 6, 19, 21})));
  REQUIRE_EQUAL(slices.size(), 1u);
  REQUIRE_EQUAL(slices[0].rows(), 8462u);
  REQUIRE_EQUAL(slices[0].columns(), 20u);

  CHECK_EQUAL(materialize(slices[0].at(0, 1)), unbox(to<data>("\"Pii6cUUq1v4\"")));
  CHECK_EQUAL(materialize(slices[0].at(0, 19)), unbox(to<data>("[]")));
  CHECK_EQUAL(materialize(slices[0].at(1, 4)), unbox(to<data>("192.168.1.255")));
  CHECK_EQUAL(materialize(slices[0].at(2, 9)), unbox(to<data>("350")));
  CHECK_EQUAL(materialize(slices[0].at(3, 14)), unbox(to<data>("\"D\"")));
}

} // namespace vast

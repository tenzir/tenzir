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

#define SUITE synopsis

#include "vast/synopsis.hpp"

#include "test.hpp"
#include "fixtures/actor_system.hpp"

#include <vector>

#include <caf/binary_deserializer.hpp>
#include <caf/binary_serializer.hpp>

using namespace std::chrono_literals;
using namespace vast;

namespace {

using interval = std::pair<timestamp, timestamp>;

const timestamp epoch;

} // namespace <anonymous>

TEST(min-max synopsis) {
  auto x = make_synopsis(timestamp_type{});
  REQUIRE(x);
  x->add(timestamp{epoch + 4s});
  x->add(timestamp{epoch + 7s});
  MESSAGE("[4,7] op 0");
  timestamp zero = epoch + 0s;
  CHECK(!x->lookup(equal, zero));
  CHECK(x->lookup(not_equal, zero));
  CHECK(!x->lookup(less, zero));
  CHECK(!x->lookup(less_equal, zero));
  CHECK(x->lookup(greater, zero));
  CHECK(x->lookup(greater_equal, zero));
  MESSAGE("[4,7] op 4");
  timestamp four = epoch + 4s;
  CHECK(x->lookup(equal, four));
  CHECK(!x->lookup(not_equal, four));
  CHECK(!x->lookup(less, four));
  CHECK(x->lookup(less_equal, four));
  CHECK(x->lookup(greater, four));
  CHECK(x->lookup(greater_equal, four));
  MESSAGE("[4,7] op 6");
  timestamp six = epoch + 6s;
  CHECK(x->lookup(equal, six));
  CHECK(!x->lookup(not_equal, six));
  CHECK(x->lookup(less, six));
  CHECK(x->lookup(less_equal, six));
  CHECK(x->lookup(greater, six));
  CHECK(x->lookup(greater_equal, six));
  MESSAGE("[4,7] op 7");
  timestamp seven = epoch + 7s;
  CHECK(x->lookup(equal, seven));
  CHECK(!x->lookup(not_equal, seven));
  CHECK(x->lookup(less, seven));
  CHECK(x->lookup(less_equal, seven));
  CHECK(!x->lookup(greater, seven));
  CHECK(x->lookup(greater_equal, seven));
  MESSAGE("[4,7] op 9");
  timestamp nine = epoch + 9s;
  CHECK(!x->lookup(equal, nine));
  CHECK(x->lookup(not_equal, nine));
  CHECK(x->lookup(less, nine));
  CHECK(x->lookup(less_equal, nine));
  CHECK(!x->lookup(greater, nine));
  CHECK(!x->lookup(greater_equal, nine));
}

FIXTURE_SCOPE(synopsis_tests, fixtures::deterministic_actor_system)

TEST(serialization) {
  CHECK_ROUNDTRIP(synopsis_ptr{});
  CHECK_ROUNDTRIP_DEREF(make_synopsis(timestamp_type{}));
}

FIXTURE_SCOPE_END()

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

#include "vast/test/test.hpp"
#include "vast/test/fixtures/actor_system.hpp"

#include <vector>

#include <caf/binary_deserializer.hpp>
#include <caf/binary_serializer.hpp>

#include "vast/boolean_synopsis.hpp"
#include "vast/synopsis_factory.hpp"
#include "vast/timestamp_synopsis.hpp"

using namespace std::chrono_literals;
using namespace vast;

namespace {

const timestamp epoch;

bool is_true(caf::optional<bool> opt) {
  return opt && *opt;
}

} // namespace <anonymous>

TEST(min-max synopsis) {
  factory<synopsis>::initialize();
  auto x = factory<synopsis>::make(timestamp_type{}, synopsis_options{});
  REQUIRE_NOT_EQUAL(x, nullptr);
  x->add(timestamp{epoch + 4s});
  x->add(timestamp{epoch + 7s});
  MESSAGE("[4,7] op 0");
  timestamp zero = epoch + 0s;
  CHECK(!is_true(x->lookup(equal, zero)));
  CHECK(is_true(x->lookup(not_equal, zero)));
  CHECK(!is_true(x->lookup(less, zero)));
  CHECK(!is_true(x->lookup(less_equal, zero)));
  CHECK(is_true(x->lookup(greater, zero)));
  CHECK(is_true(x->lookup(greater_equal, zero)));
  MESSAGE("[4,7] op 4");
  timestamp four = epoch + 4s;
  CHECK(is_true(x->lookup(equal, four)));
  CHECK(!is_true(x->lookup(not_equal, four)));
  CHECK(!is_true(x->lookup(less, four)));
  CHECK(is_true(x->lookup(less_equal, four)));
  CHECK(is_true(x->lookup(greater, four)));
  CHECK(is_true(x->lookup(greater_equal, four)));
  MESSAGE("[4,7] op 6");
  timestamp six = epoch + 6s;
  CHECK(is_true(x->lookup(equal, six)));
  CHECK(!is_true(x->lookup(not_equal, six)));
  CHECK(is_true(x->lookup(less, six)));
  CHECK(is_true(x->lookup(less_equal, six)));
  CHECK(is_true(x->lookup(greater, six)));
  CHECK(is_true(x->lookup(greater_equal, six)));
  MESSAGE("[4,7] op 7");
  timestamp seven = epoch + 7s;
  CHECK(is_true(x->lookup(equal, seven)));
  CHECK(!is_true(x->lookup(not_equal, seven)));
  CHECK(is_true(x->lookup(less, seven)));
  CHECK(is_true(x->lookup(less_equal, seven)));
  CHECK(!is_true(x->lookup(greater, seven)));
  CHECK(is_true(x->lookup(greater_equal, seven)));
  MESSAGE("[4,7] op 9");
  timestamp nine = epoch + 9s;
  CHECK(!is_true(x->lookup(equal, nine)));
  CHECK(is_true(x->lookup(not_equal, nine)));
  CHECK(is_true(x->lookup(less, nine)));
  CHECK(is_true(x->lookup(less_equal, nine)));
  CHECK(!is_true(x->lookup(greater, nine)));
  CHECK(!is_true(x->lookup(greater_equal, nine)));
}

FIXTURE_SCOPE(synopsis_tests, fixtures::deterministic_actor_system)

TEST(serialization) {
  factory<synopsis>::initialize();
  synopsis_options empty;
  CHECK_ROUNDTRIP(synopsis_ptr{});
  CHECK_ROUNDTRIP_DEREF(factory<synopsis>::make(boolean_type{}, empty));
  CHECK_ROUNDTRIP_DEREF(factory<synopsis>::make(timestamp_type{}, empty));
}

FIXTURE_SCOPE_END()

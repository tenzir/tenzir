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
#include "vast/test/synopsis.hpp"
#include "vast/test/fixtures/actor_system.hpp"

#include <caf/binary_deserializer.hpp>
#include <caf/binary_serializer.hpp>

#include "vast/boolean_synopsis.hpp"
#include "vast/synopsis_factory.hpp"
#include "vast/timestamp_synopsis.hpp"

using namespace std::chrono_literals;
using namespace vast;
using namespace vast::test;

namespace {

const timestamp epoch;

} // namespace <anonymous>

TEST(min-max synopsis) {
  using namespace nft;
  factory<synopsis>::initialize();
  auto x = factory<synopsis>::make(timestamp_type{}, synopsis_options{});
  REQUIRE_NOT_EQUAL(x, nullptr);
  x->add(timestamp{epoch + 4s});
  x->add(timestamp{epoch + 7s});
  auto verify = verifier{x};
  MESSAGE("[4,7] op 0");
  timestamp zero = epoch + 0s;
  verify(zero, {N, N, N, N, N, N, F, T, F, F, T, T});
  MESSAGE("[4,7] op 4");
  timestamp four = epoch + 4s;
  verify(four, {N, N, N, N, N, N, T, F, F, T, T, T});
  MESSAGE("[4,7] op 6");
  timestamp six = epoch + 6s;
  verify(six, {N, N, N, N, N, N, T, F, T, T, T, T});
  MESSAGE("[4,7] op 7");
  timestamp seven = epoch + 7s;
  verify(seven, {N, N, N, N, N, N, T, F, T, T, F, T});
  MESSAGE("[4,7] op 9");
  timestamp nine = epoch + 9s;
  verify(nine, {N, N, N, N, N, N, F, T, T, T, F, F});
  MESSAGE("[4,7] op {0, 4}");
  auto zero_four = data{set{zero, four}};
  auto zero_four_view = make_view(zero_four);
  verify(zero_four_view, {N, N, T, F, N, N, N, N, N, N, N, N});
  MESSAGE("[4,7] op {7, 9}");
  auto seven_nine = data{set{seven, nine}};
  auto seven_nine_view = make_view(seven_nine);
  verify(seven_nine_view, {N, N, T, F, N, N, N, N, N, N, N, N});
  MESSAGE("[4,7] op {0, 9}");
  auto zero_nine = data{set{zero, nine}};
  auto zero_nine_view = make_view(zero_nine);
  verify(zero_nine_view, {N, N, F, T, N, N, N, N, N, N, N, N});
  // Check that we don't do any implicit conversions.
  MESSAGE("[4,7] op count{5}");
  count c = 5;
  verify(c, {N, N, N, N, N, N, N, N, N, N, N, N});
  MESSAGE("[4,7] op {count{5}, 7}");
  auto heterogeneous = data{set{c, seven}};
  auto heterogeneous_view = make_view(heterogeneous);
  verify(heterogeneous_view, {N, N, T, F, N, N, N, N, N, N, N, N});
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

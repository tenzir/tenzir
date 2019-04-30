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

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/data.hpp"

#define SUITE parseable
#include "vast/test/test.hpp"

using namespace vast;
using namespace std::string_literals;

namespace {

data to_data(std::string_view str) {
  data x;
  if (!parsers::data(str, x))
    FAIL("failed to parse data from " << str);
  return x;
}

} // namespace

TEST(data) {
  MESSAGE("nil");
  CHECK_EQUAL(to_data("nil"), caf::none);
  MESSAGE("bool");
  CHECK_EQUAL(to_data("T"), data{true});
  CHECK_EQUAL(to_data("F"), data{false});
  MESSAGE("int");
  CHECK_EQUAL(to_data("+42"), data{42});
  CHECK_EQUAL(to_data("-42"), data{-42});
  MESSAGE("count");
  CHECK_EQUAL(to_data("42"), data{42u});
  MESSAGE("real");
  CHECK_EQUAL(to_data("4.2"), data{4.2});
  CHECK_EQUAL(to_data("-0.1"), data{-0.1});
  MESSAGE("string");
  CHECK_EQUAL(to_data("\"foo\""), data{"foo"});
  MESSAGE("pattern");
  CHECK_EQUAL(to_data("/foo/"), pattern{"foo"});
  MESSAGE("IP address");
  CHECK_EQUAL(to_data("10.0.0.1"), unbox(to<address>("10.0.0.1")));
  MESSAGE("port");
  CHECK_EQUAL(to_data("22/tcp"), (port{22, port::tcp}));
  MESSAGE("vector");
  CHECK_EQUAL(to_data("[]"), vector{});
  CHECK_EQUAL(to_data("[42, 4.2, nil]"), (vector{42u, 4.2, caf::none}));
  MESSAGE("set");
  CHECK_EQUAL(to_data("{}"), set{});
  CHECK_EQUAL(to_data("{42, 42, nil}"), (set{42u, caf::none}));
  MESSAGE("map");
  CHECK_EQUAL(to_data("{-}"), map{});
  CHECK_EQUAL(to_data("{+1->T,+2->F}"), (map{{1, true}, {2, false}}));
}

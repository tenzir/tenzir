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

#define SUITE value_index

#include "vast/index/list_index.hpp"

#include "vast/test/test.hpp"

#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/bitmap.hpp"
#include "vast/load.hpp"
#include "vast/save.hpp"
#include "vast/value_index_factory.hpp"

#include <caf/test/dsl.hpp>

using namespace vast;
using namespace std::string_literals;

namespace {

struct fixture {
  fixture() {
    factory<value_index>::initialize();
  }
};

} // namespace

FIXTURE_SCOPE(value_index_tests, fixture)

TEST(list) {
  auto container_type = list_type{string_type{}};
  list_index idx{container_type};
  MESSAGE("append");
  list xs{"foo", "bar"};
  REQUIRE(idx.append(make_data_view(xs)));
  xs = {"qux", "foo", "baz", "corge"};
  REQUIRE(idx.append(make_data_view(xs)));
  xs = {"bar"};
  REQUIRE(idx.append(make_data_view(xs)));
  REQUIRE(idx.append(make_data_view(xs)));
  REQUIRE(idx.append(make_data_view(xs), 7));
  MESSAGE("lookup");
  auto x = "foo"s;
  CHECK_EQUAL(to_string(*idx.lookup(ni, make_data_view(x))), "11000000");
  CHECK_EQUAL(to_string(*idx.lookup(not_ni, make_data_view(x))), "00110001");
  x = "bar";
  CHECK_EQUAL(to_string(*idx.lookup(ni, make_data_view(x))), "10110001");
  x = "not";
  CHECK_EQUAL(to_string(*idx.lookup(ni, make_data_view(x))), "00000000");
  MESSAGE("serialization");
  std::vector<char> buf;
  CHECK_EQUAL(save(nullptr, buf, idx), caf::none);
  list_index idx2{container_type};
  CHECK_EQUAL(load(nullptr, buf, idx2), caf::none);
  x = "foo";
  CHECK_EQUAL(to_string(*idx2.lookup(ni, make_data_view(x))), "11000000");
}

FIXTURE_SCOPE_END()

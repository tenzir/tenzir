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

#include "vast/value_index.hpp"

#include "vast/test/fixtures/events.hpp"
#include "vast/test/test.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/address.hpp"
#include "vast/concept/parseable/vast/data.hpp"
#include "vast/concept/parseable/vast/subnet.hpp"
#include "vast/concept/parseable/vast/time.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/bitmap.hpp"
#include "vast/load.hpp"
#include "vast/save.hpp"
#include "vast/table_slice.hpp"
#include "vast/value_index_factory.hpp"

#include <caf/test/dsl.hpp>

using namespace vast;
using namespace std::string_literals;

namespace {

struct fixture : fixtures::events {
  fixture() {
    factory<value_index>::initialize();
  }
};

} // namespace <anonymous>

FIXTURE_SCOPE(value_index_tests, fixture)

TEST(bool) {
  auto idx = factory<value_index>::make(bool_type{}, caf::settings{});
  REQUIRE_NOT_EQUAL(idx, nullptr);
  MESSAGE("append");
  REQUIRE(idx->append(make_data_view(true)));
  REQUIRE(idx->append(make_data_view(true)));
  REQUIRE(idx->append(make_data_view(false)));
  REQUIRE(idx->append(make_data_view(true)));
  REQUIRE(idx->append(make_data_view(false)));
  REQUIRE(idx->append(make_data_view(false)));
  REQUIRE(idx->append(make_data_view(false)));
  REQUIRE(idx->append(make_data_view(true)));
  MESSAGE("lookup");
  auto f = idx->lookup(equal, make_data_view(false));
  CHECK_EQUAL(to_string(unbox(f)), "00101110");
  auto t = idx->lookup(not_equal, make_data_view(false));
  CHECK_EQUAL(to_string(unbox(t)), "11010001");
  auto xs = list{true, false};
  auto multi = unbox(idx->lookup(in, make_data_view(xs)));
  CHECK_EQUAL(to_string(multi), "11111111");
  MESSAGE("serialization");
  std::string buf;
  CHECK_EQUAL(save(nullptr, buf, idx), caf::none);
  value_index_ptr idx2;
  REQUIRE_EQUAL(load(nullptr, buf, idx2), caf::none);
  t = idx2->lookup(equal, make_data_view(true));
  CHECK_EQUAL(to_string(unbox(t)), "11010001");
}

TEST(integer) {
  caf::settings opts;
  opts["base"] = "uniform(10, 20)";
  auto idx = factory<value_index>::make(integer_type{}, std::move(opts));
  REQUIRE_NOT_EQUAL(idx, nullptr);
  MESSAGE("append");
  REQUIRE(idx->append(make_data_view(-7)));
  REQUIRE(idx->append(make_data_view(42)));
  REQUIRE(idx->append(make_data_view(10000)));
  REQUIRE(idx->append(make_data_view(4711)));
  REQUIRE(idx->append(make_data_view(31337)));
  REQUIRE(idx->append(make_data_view(42)));
  REQUIRE(idx->append(make_data_view(42)));
  MESSAGE("lookup");
  auto leet = idx->lookup(equal, make_data_view(31337));
  CHECK(to_string(unbox(leet)) == "0000100");
  auto less_than_leet = idx->lookup(less, make_data_view(31337));
  CHECK(to_string(unbox(less_than_leet)) == "1111011");
  auto greater_zero = idx->lookup(greater, make_data_view(0));
  CHECK(to_string(unbox(greater_zero)) == "0111111");
  auto xs = list{42, 10, 4711};
  auto multi = unbox(idx->lookup(in, make_data_view(xs)));
  CHECK_EQUAL(to_string(multi), "0101011");
  MESSAGE("serialization");
  std::vector<char> buf;
  CHECK_EQUAL(save(nullptr, buf, idx), caf::none);
  value_index_ptr idx2;
  REQUIRE_EQUAL(load(nullptr, buf, idx2), caf::none);
  less_than_leet = idx2->lookup(less, make_data_view(31337));
  CHECK(to_string(unbox(less_than_leet)) == "1111011");
}

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

// This was the first attempt in figuring out where the bug sat. It didn't fire.
TEST(regression - checking the result single bitmap) {
  ewah_bitmap bm;
  bm.append<0>(680);
  bm.append<1>();     //  681
  bm.append<0>();     //  682
  bm.append<1>();     //  683
  bm.append<0>(36);   //  719
  bm.append<1>();     //  720
  bm.append<1>();     //  721
  for (auto i = bm.size(); i < 6464; ++i)
    bm.append<0>();
  CHECK_EQUAL(rank(bm), 4u); // regression had rank 5
  bm.append<0>();
  CHECK_EQUAL(rank(bm), 4u);
  CHECK_EQUAL(bm.size(), 6465u);
}

FIXTURE_SCOPE_END()

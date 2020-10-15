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

#include "vast/index/real_index.hpp"

#include "vast/test/test.hpp"

#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/bitmap.hpp"
#include "vast/load.hpp"
#include "vast/save.hpp"
#include "vast/value_index_factory.hpp"

#include <caf/test/dsl.hpp>

using namespace vast;

struct fixture {
  static constexpr auto nan = std::numeric_limits<double>::quiet_NaN();
  static constexpr auto pos_inf = std::numeric_limits<double>::infinity();
  static constexpr auto neg_inf = -std::numeric_limits<double>::infinity();

  fixture() : idx{real_type{}, 6, 2} {
    REQUIRE(idx.append(make_data_view(-7.8)));
    REQUIRE(idx.append(make_data_view(42.123)));
    REQUIRE(idx.append(make_data_view(10000.0)));
    REQUIRE(idx.append(make_data_view(4711.13510)));
    REQUIRE(idx.append(make_data_view(31337.3131313)));
    REQUIRE(idx.append(make_data_view(42.12258)));
    REQUIRE(idx.append(make_data_view(42.125799)));
    REQUIRE(idx.append(make_data_view(-0.8)));
    REQUIRE(idx.append(make_data_view(-0.0)));
    REQUIRE(idx.append(make_data_view(+0.0)));
    REQUIRE(idx.append(make_data_view(+0.4)));
    REQUIRE(idx.append(make_data_view(nan)));
    REQUIRE(idx.append(make_data_view(pos_inf)));
    REQUIRE(idx.append(make_data_view(neg_inf)));
  }
  real_index idx;
};

FIXTURE_SCOPE(value_index_tests, fixture)

TEST(real - nan) {
  auto result = idx.lookup(equal, make_data_view(nan));
  CHECK_EQUAL(to_string(unbox(result)), "00000000000100");
  result = idx.lookup(not_equal, make_data_view(nan));
  CHECK_EQUAL(to_string(unbox(result)), "11111111111011");
  result = idx.lookup(less, make_data_view(nan));
  CHECK_EQUAL(result.error(), ec::unsupported_operator);
  result = idx.lookup(less_equal, make_data_view(nan));
  CHECK_EQUAL(result.error(), ec::unsupported_operator);
  result = idx.lookup(greater, make_data_view(nan));
  CHECK_EQUAL(result.error(), ec::unsupported_operator);
  result = idx.lookup(greater_equal, make_data_view(nan));
  CHECK_EQUAL(result.error(), ec::unsupported_operator);
}

TEST(real - zero) {
  auto result = idx.lookup(equal, make_data_view(0.0));
  CHECK_EQUAL(to_string(unbox(result)), "00000000110000");
  result = idx.lookup(not_equal, make_data_view(0.0));
  CHECK_EQUAL(to_string(unbox(result)), "11111111001111");
  result = idx.lookup(less, make_data_view(0.0));
  CHECK_EQUAL(to_string(unbox(result)), "10000001000001");
  result = idx.lookup(less_equal, make_data_view(0.0));
  CHECK_EQUAL(to_string(unbox(result)), "10000001110001");
  result = idx.lookup(greater, make_data_view(0.0));
  CHECK_EQUAL(to_string(unbox(result)), "01111110001010");
  result = idx.lookup(greater_equal, make_data_view(0.0));
  CHECK_EQUAL(to_string(unbox(result)), "01111110111010");
}

TEST(real - pos_inf) {
  auto result = idx.lookup(equal, make_data_view(pos_inf));
  CHECK_EQUAL(to_string(unbox(result)), "00000000000010");
  result = idx.lookup(not_equal, make_data_view(pos_inf));
  CHECK_EQUAL(to_string(unbox(result)), "11111111111101");
  result = idx.lookup(less, make_data_view(pos_inf));
  CHECK_EQUAL(to_string(unbox(result)), "11111111111001");
  result = idx.lookup(less_equal, make_data_view(pos_inf));
  CHECK_EQUAL(to_string(unbox(result)), "11111111111011");
  result = idx.lookup(greater, make_data_view(pos_inf));
  CHECK_EQUAL(to_string(unbox(result)), "00000000000000");
  result = idx.lookup(greater_equal, make_data_view(pos_inf));
  CHECK_EQUAL(to_string(unbox(result)), "00000000000010");
}

TEST(real - neg_inf) {
  auto result = idx.lookup(equal, make_data_view(neg_inf));
  CHECK_EQUAL(to_string(unbox(result)), "00000000000001");
  result = idx.lookup(not_equal, make_data_view(neg_inf));
  CHECK_EQUAL(to_string(unbox(result)), "11111111111110");
  result = idx.lookup(less, make_data_view(neg_inf));
  CHECK_EQUAL(to_string(unbox(result)), "00000000000000");
  result = idx.lookup(less_equal, make_data_view(neg_inf));
  CHECK_EQUAL(to_string(unbox(result)), "00000000000001");
  result = idx.lookup(greater, make_data_view(neg_inf));
  CHECK_EQUAL(to_string(unbox(result)), "11111111111010");
  result = idx.lookup(greater_equal, make_data_view(neg_inf));
  CHECK_EQUAL(to_string(unbox(result)), "11111111111011");
}

//    (-7.8);
//    (42.123);
//    (10000.0);
//    (4711.13510);
//    (31337.3131313);
//    (42.12258);
//    (42.125799);
//    (-0.8);
//    (-0.0);
//    (+0.0);
//    (+0.4);
//    (nan);
//    (pos_inf);
//    (neg_inf);
TEST(real - normal and subnormal) {
  auto result = idx.lookup(less, make_data_view(100.0));
  CHECK_EQUAL(to_string(unbox(result)), "11000111111001");
  result = idx.lookup(less, make_data_view(43.0));
  CHECK_EQUAL(to_string(unbox(result)), "11000111111001");
  result = idx.lookup(less, make_data_view(0.9));
  CHECK_EQUAL(to_string(unbox(result)), "10000001111001");
  result = idx.lookup(equal, make_data_view(10'000.001));
  CHECK_EQUAL(to_string(unbox(result)), "00100000000000");
  result = idx.lookup(greater_equal, make_data_view(42.0));
  CHECK_EQUAL(to_string(unbox(result)), "01111110000010");
  result = idx.lookup(equal, make_data_view(4711.14));
  CHECK_EQUAL(to_string(unbox(result)), "00010000000000");
  result = idx.lookup(not_equal, make_data_view(4711.14));
  CHECK_EQUAL(to_string(unbox(result)), "11101111111111");
}

TEST(real - serialization) {
  std::vector<char> buf;
  CHECK_EQUAL(save(nullptr, buf, idx), caf::none);
  auto idx2 = real_index{real_type{}};
  REQUIRE_EQUAL(load(nullptr, buf, idx2), caf::none);
  auto result = idx2.lookup(not_equal, make_data_view(4711.14));
  CHECK_EQUAL(to_string(unbox(result)), "1110111");
}

FIXTURE_SCOPE_END()

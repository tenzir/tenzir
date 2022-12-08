//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#define SUITE value_index

#include "vast/index/arithmetic_index.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/address.hpp"
#include "vast/concept/parseable/vast/data.hpp"
#include "vast/concept/parseable/vast/subnet.hpp"
#include "vast/concept/parseable/vast/time.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/bitmap.hpp"
#include "vast/detail/legacy_deserialize.hpp"
#include "vast/detail/serialize.hpp"
#include "vast/table_slice.hpp"
#include "vast/test/test.hpp"
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

TEST(real with custom binner) {
  using index_type = arithmetic_index<real, precision_binner<6, 2>>;
  caf::settings opts;
  opts["base"] = "uniform64(10)";
  auto idx = index_type{type{real_type{}}, opts};
  MESSAGE("append");
  REQUIRE(idx.append(make_data_view(-7.8)));
  REQUIRE(idx.append(make_data_view(42.123)));
  REQUIRE(idx.append(make_data_view(10000.0)));
  REQUIRE(idx.append(make_data_view(4711.13510)));
  REQUIRE(idx.append(make_data_view(31337.3131313)));
  REQUIRE(idx.append(make_data_view(42.12258)));
  REQUIRE(idx.append(make_data_view(42.125799)));
  MESSAGE("lookup");
  auto result = idx.lookup(relational_operator::less, make_data_view(100.0));
  CHECK_EQUAL(to_string(unbox(result)), "1100011");
  result = idx.lookup(relational_operator::less, make_data_view(43.0));
  CHECK_EQUAL(to_string(unbox(result)), "1100011");
  result = idx.lookup(relational_operator::greater_equal, make_data_view(42.0));
  CHECK_EQUAL(to_string(unbox(result)), "0111111");
  result = idx.lookup(relational_operator::not_equal, make_data_view(4711.14));
  CHECK_EQUAL(to_string(unbox(result)), "1110111");
  MESSAGE("serialization");
  caf::byte_buffer buf;
  CHECK(detail::serialize(buf, idx));
  auto idx2 = index_type{type{real_type{}}, opts};
  REQUIRE_EQUAL(detail::legacy_deserialize(buf, idx2), true);
  result = idx2.lookup(relational_operator::not_equal, make_data_view(4711.14));
  CHECK_EQUAL(to_string(unbox(result)), "1110111");
}

TEST(duration) {
  using namespace std::chrono;
  caf::settings opts;
  opts["base"] = "uniform64(10)";
  // Default binning gives granularity of seconds.
  auto idx = arithmetic_index<vast::duration>{type{duration_type{}}, opts};
  MESSAGE("append");
  REQUIRE(idx.append(make_data_view(milliseconds(1000))));
  REQUIRE(idx.append(make_data_view(milliseconds(2000))));
  REQUIRE(idx.append(make_data_view(milliseconds(3000))));
  REQUIRE(idx.append(make_data_view(milliseconds(911))));
  REQUIRE(idx.append(make_data_view(milliseconds(1011))));
  REQUIRE(idx.append(make_data_view(milliseconds(1411))));
  REQUIRE(idx.append(make_data_view(milliseconds(2222))));
  REQUIRE(idx.append(make_data_view(milliseconds(2322))));
  MESSAGE("lookup");
  auto lookup = [&](relational_operator op, auto dv) {
    return to_string(unbox(idx.lookup(op, dv)));
  };
  auto hun = make_data_view(milliseconds(1034));
  auto twelve = make_data_view(milliseconds(1200));
  auto twokay = make_data_view(milliseconds(2000));
  CHECK_EQUAL(lookup(relational_operator::equal, hun), "10001100");
  CHECK_EQUAL(lookup(relational_operator::less_equal, twokay), "11011111");
  CHECK_EQUAL(lookup(relational_operator::greater, twelve), "11101111");
  CHECK_EQUAL(lookup(relational_operator::greater_equal, twelve), "11101111");
  CHECK_EQUAL(lookup(relational_operator::less, twelve), "10011100");
  CHECK_EQUAL(lookup(relational_operator::less_equal, twelve), "10011100");
}

TEST(time) {
  caf::settings opts;
  opts["base"] = "uniform64(10)";
  arithmetic_index<vast::time> idx{type{time_type{}}, opts};
  auto ts = to<vast::time>("2014-01-16+05:30:15");
  MESSAGE("append");
  REQUIRE(idx.append(make_data_view(unbox(ts))));
  ts = to<vast::time>("2014-01-16+05:30:12");
  REQUIRE(idx.append(make_data_view(unbox(ts))));
  ts = to<vast::time>("2014-01-16+05:30:15");
  REQUIRE(idx.append(make_data_view(unbox(ts))));
  ts = to<vast::time>("2014-01-16+05:30:18");
  REQUIRE(idx.append(make_data_view(unbox(ts))));
  ts = to<vast::time>("2014-01-16+05:30:15");
  REQUIRE(idx.append(make_data_view(unbox(ts))));
  ts = to<vast::time>("2014-01-16+05:30:19");
  REQUIRE(idx.append(make_data_view(unbox(ts))));
  MESSAGE("lookup");
  ts = to<vast::time>("2014-01-16+05:30:15");
  auto fifteen
    = idx.lookup(relational_operator::equal, make_data_view(unbox(ts)));
  CHECK(to_string(unbox(fifteen)) == "101010");
  ts = to<vast::time>("2014-01-16+05:30:20");
  auto twenty
    = idx.lookup(relational_operator::less, make_data_view(unbox(ts)));
  CHECK(to_string(unbox(twenty)) == "111111");
  ts = to<vast::time>("2014-01-16+05:30:18");
  auto eighteen
    = idx.lookup(relational_operator::greater_equal, make_data_view(unbox(ts)));
  CHECK(to_string(unbox(eighteen)) == "000101");
  MESSAGE("serialization");
  caf::byte_buffer buf;
  CHECK(detail::serialize(buf, idx));
  arithmetic_index<vast::time> idx2{type{time_type{}}, opts};
  CHECK_EQUAL(detail::legacy_deserialize(buf, idx2), true);
  eighteen = idx2.lookup(relational_operator::greater_equal,
                         make_data_view(unbox(ts)));
  CHECK(to_string(*eighteen) == "000101");
}

TEST(none values - arithmetic) {
  auto idx = factory<value_index>::make(type{count_type{}}, caf::settings{});
  REQUIRE_NOT_EQUAL(idx, nullptr);
  REQUIRE(idx->append(make_data_view(caf::none)));
  REQUIRE(idx->append(make_data_view(integer{42})));
  REQUIRE(idx->append(make_data_view(integer{43})));
  REQUIRE(idx->append(make_data_view(caf::none)));
  REQUIRE(idx->append(make_data_view(caf::none)));
  auto bm = idx->lookup(relational_operator::less, make_data_view(integer{50}));
  CHECK_EQUAL(to_string(unbox(bm)), "01100");
  bm = idx->lookup(relational_operator::greater, make_data_view(integer{42}));
  CHECK_EQUAL(to_string(unbox(bm)), "00100");
}

FIXTURE_SCOPE_END()

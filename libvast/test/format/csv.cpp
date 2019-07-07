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

#include "vast/format/csv.hpp"

#define SUITE format

#include "vast/test/test.hpp"

#include "vast/test/fixtures/actor_system.hpp"
#include "vast/test/fixtures/events.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/address.hpp"
#include "vast/concept/parseable/vast/time.hpp"
#include "vast/default_table_slice_builder.hpp"

using namespace vast;
using namespace std::string_literals;

namespace {

auto l0 = record_type{{"ts", timestamp_type{}},
                      {"addr", address_type{}},
                      {"port", count_type{}}}
            .name("l0");

std::string_view l0_log0 = R"__(ts,addr,port
2011-08-12T13:00:36.349948Z,147.32.84.165,1027
2011-08-12T13:08:01.360925Z,147.32.84.165,3101
2011-08-12T13:08:01.360925Z,147.32.84.165,1029
2011-08-12T13:09:35.498887Z,147.32.84.165,1029
2011-08-12T13:14:36.012344Z,147.32.84.165,1041
2011-08-12T14:59:11.994970Z,147.32.84.165,1046
2011-08-12T14:59:12.448311Z,147.32.84.165,1047
2011-08-13T13:04:24.640406Z,147.32.84.165,1089)__";

std::string_view l0_log1 = R"__(ts,addr,port
2011-08-12T13:00:36.349948Z,147.32.84.165,1027
2011-08-12T13:08:01.360925Z,147.32.84.165,
2011-08-12T13:08:01.360925Z,,1029
2011-08-12T13:09:35.498887Z,147.32.84.165,1029
2011-08-12T13:14:36.012344Z,147.32.84.165,1041
,147.32.84.165,1046
,147.32.84.165,
,,)__";

} // namespace

FIXTURE_SCOPE(csv_reader_tests, fixtures::deterministic_actor_system)

TEST(csv reader) {
  using reader_type = format::csv::reader;
  auto in = std::make_unique<std::istringstream>(std::string{l0_log0});
  reader_type reader{defaults::system::table_slice_type, std::move(in)};
  schema s;
  REQUIRE(s.add(l0));
  reader.schema(s);
  std::vector<table_slice_ptr> slices;
  auto add_slice = [&](table_slice_ptr ptr) {
    slices.emplace_back(std::move(ptr));
  };
  {
    auto [err, num] = reader.read(8, 5, add_slice);
    CHECK_EQUAL(err, caf::none);
    CHECK_EQUAL(num, 8);
    CHECK(slices[1]->at(0, 0)
          == data{unbox(to<timestamp>("2011-08-12T14:59:11.994970Z"))});
    CHECK(slices[1]->at(1, 2) == port{1047});
  }
  in = std::make_unique<std::istringstream>(std::string{l0_log1});
  reader.reset(std::move(in));
  slices.clear();
  {
    auto [err, num] = reader.read(8, 5, add_slice);
    CHECK_EQUAL(err, caf::none);
    CHECK_EQUAL(num, 8);
    CHECK(slices[1]->at(0, 1) == data{unbox(to<address>("147.32.84.165"))});
    CHECK(slices[1]->at(1, 2) == data{caf::none});
  }
}

FIXTURE_SCOPE_END()

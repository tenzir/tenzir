//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/partition_sketch_builder.hpp"

#define SUITE partition_sketch
#include "vast/concept/convertible/data.hpp"
#include "vast/data.hpp"
#include "vast/test/fixtures/events.hpp"
#include "vast/test/test.hpp"

#include <caf/test/dsl.hpp>

using namespace vast;

namespace {

auto example_index_config = R"__(
rules:
  - targets:
      - orig_h
      - zeek.conn.id.resp_h
    fp-rate: 0.005
  - targets:
      - :address
    fp-rate: 0.1
)__";

struct fixture : fixtures::events {
  fixture() {
    const auto yaml = unbox(from_yaml(example_index_config));
    REQUIRE_EQUAL(convert(yaml, config), caf::none);
  }

  index_config config;
};

} // namespace

FIXTURE_SCOPE(partition_sketch_tests, fixture)

TEST(builder instantiation) {
  auto builder = unbox(partition_sketch_builder::make(config));
  auto err = builder.add(zeek_conn_log[0]);
  CHECK_EQUAL(err, caf::none);
  fmt::print("+++++++++++++++++++++\n");
  for (auto field : builder.fields())
    fmt::print("{}\n", field);
  fmt::print("---------------------\n");
  for (auto type : builder.types())
    fmt::print("{}\n", type);
  fmt::print("+++++++++++++++++++++\n");
}

FIXTURE_SCOPE_END()

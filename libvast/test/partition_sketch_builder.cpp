//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/partition_sketch_builder.hpp"

#define SUITE partition_sketch
#include "vast/concept/convertible/data.hpp" //FIXME: remove
#include "vast/data.hpp"                     //FIXME: remove
#include "vast/test/test.hpp"

#include <caf/test/dsl.hpp>

using namespace vast;

namespace {

auto example_index_config = R"__(
rules:
  - targets:
      - suricata.dns.dns.rrname
      - :address
    fp-rate: 0.005
)__";

struct fixture {
  fixture() {
    const auto yaml = unbox(from_yaml(example_index_config));
    REQUIRE_EQUAL(convert(yaml, config), caf::none);
    REQUIRE_EQUAL(config.rules.size(), 1u);
    const auto& rule = config.rules[0];
    REQUIRE_EQUAL(rule.targets.size(), 2u);
    REQUIRE_EQUAL(rule.targets[0], "suricata.dns.dns.rrname");
    REQUIRE_EQUAL(rule.fp_rate, 0.005);
  }

  index_config config;
};

} // namespace

FIXTURE_SCOPE(partition_sketch_tests, fixture)

TEST(builder instantiation) {
  auto builder = partition_sketch_builder{config};
  // TODO: test
}

FIXTURE_SCOPE_END()

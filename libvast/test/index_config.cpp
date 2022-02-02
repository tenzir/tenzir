//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#define SUITE index_config

#include "vast/index_config.hpp"

#include "vast/concept/convertible/data.hpp"
#include "vast/data.hpp"
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
  - targets:
      - zeek.conn.id.orig_h
)__";

} // namespace

TEST(example configuration) {
  const auto yaml = unbox(from_yaml(example_index_config));
  index_config config;
  REQUIRE_EQUAL(convert(yaml, config), caf::none);
  REQUIRE_EQUAL(config.rules.size(), 2u);
  const auto& rule0 = config.rules[0];
  REQUIRE_EQUAL(rule0.targets.size(), 2u);
  CHECK_EQUAL(rule0.targets[0], "suricata.dns.dns.rrname");
  CHECK_EQUAL(rule0.fp_rate, 0.005);
  const auto& rule1 = config.rules[1];
  REQUIRE_EQUAL(rule1.targets.size(), 1u);
  CHECK_EQUAL(rule1.targets[0], "zeek.conn.id.orig_h");
  CHECK_EQUAL(rule1.fp_rate, 0.01); // default
}

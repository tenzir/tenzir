//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/index_config.hpp"

#include "tenzir/concept/convertible/data.hpp"
#include "tenzir/data.hpp"
#include "tenzir/qualified_record_field.hpp"
#include "tenzir/test/test.hpp"

using namespace tenzir;

namespace {

auto example_index_config = R"__(
rules:
  - targets:
      - suricata.dns.dns.rrname
      - :addr
    fp-rate: 0.005
  - targets:
      - zeek.conn.id.orig_h
    partition-index: false
)__";

const tenzir::type schema{
  "y",
  tenzir::record_type{
    {"x", tenzir::uint64_type{}},
    {"y", tenzir::type{"foo", tenzir::uint64_type{}}},
  },
};

} // namespace

TEST("example configuration") {
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
  CHECK_EQUAL(rule1.fp_rate, 0.01);                // default
  CHECK_EQUAL(rule0.create_partition_index, true); // default
  CHECK_EQUAL(rule1.create_partition_index, false);
}

TEST("should_create_partition_index will return true for empty rules") {
  CHECK_EQUAL(should_create_partition_index({}, {}), true);
}

TEST("should_create_partition_index will return true if no field name in "
     "rules") {
  qualified_record_field in{schema, {0u}};
  CHECK_EQUAL(should_create_partition_index(in, {}), true);
}

TEST("should_create_partition_index will use create_partition_index from "
     "config if field name is in the rule") {
  qualified_record_field in{schema, {0u}};
  auto rules = std::vector{
    index_config::rule{.targets = {"y.x"}, .create_partition_index = false}};
  CHECK_EQUAL(should_create_partition_index(in, rules),
              rules.front().create_partition_index);
  // change config to true
  rules.front().create_partition_index = true;
  CHECK_EQUAL(should_create_partition_index(in, rules),
              rules.front().create_partition_index);
}

TEST("should_create_partition_index will will use create_partition_index from "
     "config if type is in the rule") {
  qualified_record_field in_x{schema, {0u}};
  qualified_record_field in_y{schema, {1u}};
  auto rules_x = std::vector{
    index_config::rule{.targets = {":uint64"}, .create_partition_index = false},
  };
  auto rules_y = std::vector{
    index_config::rule{.targets = {":foo"}, .create_partition_index = false},
  };
  CHECK_EQUAL(should_create_partition_index(in_x, rules_x), false);
  CHECK_EQUAL(should_create_partition_index(in_x, rules_y), true);
  CHECK_EQUAL(should_create_partition_index(in_y, rules_x), false);
  CHECK_EQUAL(should_create_partition_index(in_y, rules_y), false);
  // change config to true
  rules_x.front().create_partition_index = true;
  rules_y.front().create_partition_index = true;
  CHECK_EQUAL(should_create_partition_index(in_x, rules_x), true);
  CHECK_EQUAL(should_create_partition_index(in_x, rules_y), true);
  CHECK_EQUAL(should_create_partition_index(in_y, rules_x), true);
  CHECK_EQUAL(should_create_partition_index(in_y, rules_y), true);
}

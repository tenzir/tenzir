//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/partition_sketch_builder.hpp"

#define SUITE partition_sketch_builder

#include "vast/concept/convertible/data.hpp"
#include "vast/data.hpp"
#include "vast/test/fixtures/events.hpp"
#include "vast/test/test.hpp"

#include <caf/test/dsl.hpp>

#include <set>

using namespace vast;

namespace {

// TODO: Add a config rule for `suricata.dns`
auto example_index_config = R"__(
use_sketches: true
rules:
  - targets:
      - id.orig_h
      - zeek.conn.id.resp_h
    fp-rate: 0.005
  - targets:
      - :address
    fp-rate: 0.1
)__";

auto invalid_config = R"__(
rules:
  - targets:
      - id.orig_h
      - id.orig_h
)__";

} // namespace

TEST(duplicate targets) {
  const auto yaml = unbox(from_yaml(invalid_config));
  index_config config;
  auto err = convert(yaml, config);
  REQUIRE_EQUAL(convert(yaml, config), caf::none);
  auto dummy_layout = vast::record_type{{"foo", vast::bool_type{}}};
  auto builder
    = partition_sketch_builder::make(vast::type{dummy_layout}, config);
  REQUIRE(!builder);
  CHECK_EQUAL(builder.error(), ec::unspecified);
}

FIXTURE_SCOPE(partition_sketch_tests, fixtures::events)

TEST(builder instantiation) {
  index_config config;
  const auto yaml = unbox(from_yaml(example_index_config));
  REQUIRE_EQUAL(convert(yaml, config), caf::none);
  auto slice = zeek_conn_log[0];
  auto layout = slice.layout();
  auto builder = unbox(partition_sketch_builder::make(layout, config));
  auto err = builder.add(slice);
  CHECK_EQUAL(err, caf::none);
  MESSAGE("check if all field builders have been instantiated");
  auto actual_fields = std::set<std::string>{};
  auto expected_fields
    = std::set<std::string>{"id.orig_h", "zeek.conn.id.resp_h"};
  for (auto field : builder.fields())
    actual_fields.insert(std::string{field});
  MESSAGE("check if all type builders have been instantiated");
  auto actual_types = std::set<vast::type>{};
  // This is the list of unique type names when traversing the Zeek connection
  // log columns. Note that 'port' is a type alias that receives its own sketch
  // as derivative of 'count'.
  auto expected_types = std::set<vast::type>{};
  auto partition_layout = caf::get<record_type>(layout);
  for (auto const& field : partition_layout.fields())
    expected_types.insert(field.type);
  for (auto const& type : builder.types())
    actual_types.insert(type);
  CHECK_EQUAL(actual_types, expected_types);
}

FIXTURE_SCOPE_END()

//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

// NOTE: This file contains an example for using the CAF testing framework, and
// does not contain any meaningful tests for the example plugin. It merely
// exists to show how to setup unit tests.

#define CAF_SUITE transform_plugin

#include <vast/system/make_transform.hpp>

#include <caf/test/unit_test.hpp>

const std::string config = R"_(
vast:
  transforms:
    example_transform:
      - example_transform_step:
        field: foo
  transform-triggers:
    import:
      - transform: example_transform
        location: server
        events: vast.test
)_";

// Verify that we can use the transform names to load
CAF_TEST(load plugins from config) {
  auto yaml = vast::from_yaml(config);
  REQUIRE(yaml);
  auto rec = caf::get_if<vast::record>(&*yaml);
  REQUIRE(rec);
  auto settings = vast::to<caf::settings>(*rec);
  REQUIRE(settings);
  auto transforms = parse_transforms(location, *settings);
  CAF_REQUIRE(transforms);
}
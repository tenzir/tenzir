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

#include <vast/concept/convertible/to.hpp>
#include <vast/data.hpp>
#include <vast/system/make_legacy_pipelines.hpp>
#include <vast/test/test.hpp>

#include <caf/settings.hpp>

namespace {

const std::string config = R"_(
vast:
  pipelines:
    my-pipeline:
      - example-pipeline: {}
  pipeline-triggers:
    import:
      - pipeline: my-pipeline
        location: server
        events:
          - vast.test
)_";

} // namespace

// Verify that we can use the pipeline names to load
TEST(load plugins from config) {
  auto yaml = vast::from_yaml(config);
  REQUIRE(yaml);
  auto* rec = caf::get_if<vast::record>(&*yaml);
  REQUIRE(rec);
  auto settings = vast::to<caf::settings>(*rec);
  REQUIRE(settings);
  auto client_source_pipelines = vast::system::make_pipelines(
    vast::system::pipelines_location::client_source, *settings);
  CHECK(client_source_pipelines);
  auto server_import_pipelines = vast::system::make_pipelines(
    vast::system::pipelines_location::server_import, *settings);
  CHECK(server_import_pipelines);
  auto server_export_pipelines = vast::system::make_pipelines(
    vast::system::pipelines_location::server_export, *settings);
  REQUIRE(server_export_pipelines);
  auto client_sink_pipelines = vast::system::make_pipelines(
    vast::system::pipelines_location::client_sink, *settings);
  REQUIRE(client_sink_pipelines);
}

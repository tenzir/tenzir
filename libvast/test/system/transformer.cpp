//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#define SUITE transformer

#include "vast/system/transformer.hpp"

#include "vast/arrow_table_slice_builder.hpp"
#include "vast/concept/convertible/to.hpp"
#include "vast/concept/parseable/vast/data.hpp"
#include "vast/data.hpp"
#include "vast/detail/framed.hpp"
#include "vast/detail/logger_formatters.hpp"
#include "vast/detail/spawn_container_source.hpp"
#include "vast/system/make_pipelines.hpp"
#include "vast/table_slice_builder_factory.hpp"
#include "vast/test/fixtures/actor_system_and_events.hpp"
#include "vast/test/fixtures/table_slices.hpp"
#include "vast/test/test.hpp"
#include "vast/uuid.hpp"

#include <caf/attach_stream_sink.hpp>

namespace {

const std::string pipeline_config = R"_(
vast:
  pipelines:
    delete_uid:
      - drop:
          fields:
            - uid
    replace_uid:
      - replace:
          fields:
            uid: xxx

  pipeline-triggers:
    import:
      - pipeline: delete_uid
        location: server
        events: [vast.test]
    export:
      # Using the deprecated 'transform' key instead of 'pipeline'
      # to ensure that is still supported.
      - transform: replace_uid
        location: client
        events: [vast.test]
)_";

} // namespace

vast::system::stream_sink_actor<vast::table_slice>::behavior_type
dummy_sink(vast::system::stream_sink_actor<vast::table_slice>::pointer self,
           vast::table_slice* result) {
  return {
    [=](caf::stream<vast::table_slice> in) {
      auto sink = caf::attach_stream_sink(
        self, in,
        [=](caf::unit_t&) {
          // nop
        },
        [=](caf::unit_t&, vast::table_slice&& x) {
          *result = std::move(x);
        });
      return caf::inbound_stream_slot<vast::table_slice>{sink.inbound_slot()};
    },
  };
}

struct transformer_fixture
  : public fixtures::deterministic_actor_system_and_events {
  transformer_fixture()
    : fixtures::deterministic_actor_system_and_events(
      VAST_PP_STRINGIFY(SUITE)) {
    vast::factory<vast::table_slice_builder>::initialize();
  }

  // Creates a table slice with a single string field and random data.
  static std::vector<vast::detail::framed<vast::table_slice>>
  make_pipelines_testdata() {
    auto layout = vast::type{
      "vast.test",
      vast::record_type{
        {"uid", vast::string_type{}},
        {"index", vast::integer_type{}},
      },
    };
    auto builder = vast::factory<vast::table_slice_builder>::make(
      vast::defaults::import::table_slice_type, layout);
    REQUIRE(builder);
    for (int i = 0; i < 10; ++i) {
      auto uuid = vast::uuid::random();
      auto str = fmt::format("{}", uuid);
      REQUIRE(builder->add(str, vast::integer{i}));
    }
    return {builder->finish()};
  }
};

std::vector<vast::pipeline>
pipelines_from_string(vast::system::pipelines_location location,
                      const std::string& str) {
  auto yaml = vast::from_yaml(str);
  REQUIRE(yaml);
  auto* rec = caf::get_if<vast::record>(&*yaml);
  REQUIRE(rec);
  auto settings = vast::to<caf::settings>(*rec);
  REQUIRE(settings);
  auto pipelines = make_pipelines(location, *settings);
  REQUIRE_NOERROR(pipelines);
  return std::move(*pipelines);
}

FIXTURE_SCOPE(transformer_tests, transformer_fixture)

TEST(transformer config) {
  auto client_sink_pipelines = pipelines_from_string(
    vast::system::pipelines_location::client_sink, pipeline_config);
  auto client_source_pipelines = pipelines_from_string(
    vast::system::pipelines_location::client_source, pipeline_config);
  auto server_import_pipelines = pipelines_from_string(
    vast::system::pipelines_location::server_import, pipeline_config);
  auto server_export_pipelines = pipelines_from_string(
    vast::system::pipelines_location::server_export, pipeline_config);

  CHECK_EQUAL(client_sink_pipelines.size(), 1ull);
  CHECK_EQUAL(client_source_pipelines.size(), 0ull);
  CHECK_EQUAL(server_import_pipelines.size(), 1ull);
  CHECK_EQUAL(server_export_pipelines.size(), 0ull);
}

TEST(transformer) {
  vast::table_slice result;
  auto snk = this->self->spawn(dummy_sink, &result);
  // This should return one pipeline, `delete_uid`.
  auto pipelines = pipelines_from_string(
    vast::system::pipelines_location::server_import, pipeline_config);
  REQUIRE_EQUAL(pipelines.size(), 1ull);
  CHECK_EQUAL(pipelines[0].name(), "delete_uid");
  CHECK(pipelines[0].applies_to("vast.test"));
  auto transformer = self->spawn(vast::system::transformer, "test_transformer",
                                 std::move(pipelines));
  this->self->send(transformer, snk);
  run();
  auto slices = make_pipelines_testdata();
  REQUIRE_EQUAL(slices.size(), 1ull);
  vast::detail::spawn_container_source(self->system(), slices, transformer);
  run(); // The dummy_sink should store the transformed table slice in `result`.
  auto layout_after_delete = vast::type{
    "vast.test",
    vast::record_type{
      {"index", vast::integer_type{}},
    },
  };
  auto& slice = slices[0];
  CHECK_EQUAL(slice.header, vast::detail::stream_control_header::data);
  CHECK_EQUAL(slice.body.rows(), result.rows());
  CHECK_EQUAL(result.layout(), layout_after_delete);
  CHECK_EQUAL(slice.body.offset(), result.offset());
  self->send_exit(transformer, caf::exit_reason::user_shutdown);
}

FIXTURE_SCOPE_END()

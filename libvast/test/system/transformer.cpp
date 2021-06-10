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
#include "vast/detail/logger_formatters.hpp"
#include "vast/detail/spawn_container_source.hpp"
#include "vast/msgpack_table_slice_builder.hpp"
#include "vast/system/make_transforms.hpp"
#include "vast/test/fixtures/actor_system_and_events.hpp"
#include "vast/test/fixtures/table_slices.hpp"
#include "vast/test/test.hpp"
#include "vast/uuid.hpp"

namespace {

const std::string transform_config = R"_(
vast:
  transforms:
    delete_uid:
      - delete:
          field: uid
    replace_uid:
      - replace:
          field: uid
          value: "xxx"

  transform-triggers:
    import:
      - transform: delete_uid
        location: server
        events: [vast.test]
    export:
      - transform: replace_uid
        location: client
        events: [vast.test]
)_";

}

vast::system::stream_sink_actor<
  vast::system::stream_controlled<vast::table_slice>>::behavior_type
dummy_sink(vast::system::stream_sink_actor<
             vast::system::stream_controlled<vast::table_slice>>::pointer self,
           vast::table_slice* result) {
  return {
    [=](caf::stream<vast::system::stream_controlled<vast::table_slice>> in) {
      auto sink = self->make_sink(
        in,
        [=](caf::unit_t&) {
          // nop
        },
        [=](caf::unit_t&,
            vast::system::stream_controlled<vast::table_slice>&& x) {
          REQUIRE(caf::holds_alternative<vast::table_slice>(x));
          *result = std::move(caf::get<vast::table_slice>(x));
        });
      return caf::inbound_stream_slot<
        vast::system::stream_controlled<vast::table_slice>>{
        sink.inbound_slot()};
    },
  };
}

struct transformer_fixture
  : public fixtures::deterministic_actor_system_and_events {
  transformer_fixture() {
    vast::factory<vast::table_slice_builder>::initialize();
  }

  // Creates a table slice with a single string field and random data.
  static std::vector<vast::system::stream_controlled<vast::table_slice>>
  make_transforms_testdata() {
    auto layout = vast::record_type{{"uid", vast::string_type{}},
                                    {"index", vast::integer_type{}}}
                    .name("vast.test");
    auto builder = vast::factory<vast::table_slice_builder>::make(
      vast::defaults::import::table_slice_type, layout);
    REQUIRE(builder);
    for (int i = 0; i < 10; ++i) {
      auto uuid = vast::uuid::random();
      auto str = fmt::format("{}", uuid);
      REQUIRE(builder->add(str, vast::integer{i}));
    }
    auto slice = builder->finish();
    return {vast::system::stream_controlled<vast::table_slice>{slice}};
  }
};

std::vector<vast::transform>
transforms_from_string(vast::system::transforms_location location,
                       const std::string& str) {
  auto yaml = vast::from_yaml(str);
  REQUIRE(yaml);
  auto* rec = caf::get_if<vast::record>(&*yaml);
  REQUIRE(rec);
  auto settings = vast::to<caf::settings>(*rec);
  REQUIRE(settings);
  auto transforms = make_transforms(location, *settings);
  if (!transforms) {
    std::cout << vast::render(transforms.error()) << std::endl;
  }
  REQUIRE(transforms);
  return std::move(*transforms);
}

FIXTURE_SCOPE(transformer_tests, transformer_fixture)

TEST(transformer config) {
  auto client_sink_transforms = transforms_from_string(
    vast::system::transforms_location::client_sink, transform_config);
  auto client_source_transforms = transforms_from_string(
    vast::system::transforms_location::client_source, transform_config);
  auto server_import_transforms = transforms_from_string(
    vast::system::transforms_location::server_import, transform_config);
  auto server_export_transforms = transforms_from_string(
    vast::system::transforms_location::server_export, transform_config);

  CHECK_EQUAL(client_sink_transforms.size(), 1ull);
  CHECK_EQUAL(client_source_transforms.size(), 0ull);
  CHECK_EQUAL(server_import_transforms.size(), 1ull);
  CHECK_EQUAL(server_export_transforms.size(), 0ull);
}

TEST(transformer) {
  vast::table_slice result;
  auto snk = this->self->spawn(dummy_sink, &result);
  // This should return one transform, `delete_uid`.
  auto transforms = transforms_from_string(
    vast::system::transforms_location::server_import, transform_config);
  REQUIRE_EQUAL(transforms.size(), 1ull);
  CHECK_EQUAL(transforms[0].name(), "delete_uid");
  CHECK_EQUAL(transforms[0].event_types(), std::vector<std::string>{"vast."
                                                                    "test"});
  auto transformer = self->spawn(vast::system::transformer, "test_transformer",
                                 std::move(transforms));
  this->self->send(transformer, snk);
  run();
  auto slices = make_transforms_testdata();
  REQUIRE_EQUAL(slices.size(), 1ull);
  vast::detail::spawn_container_source(self->system(), slices, transformer);
  run(); // The dummy_sink should store the transformed table slice in `result`.
  auto layout_after_delete
    = vast::record_type{{"index", vast::integer_type{}}}.name("vast.test");
  REQUIRE(caf::holds_alternative<vast::table_slice>(slices[0]));
  auto& slice = caf::get<vast::table_slice>(slices[0]);
  CHECK_EQUAL(slice.rows(), result.rows());
  CHECK_EQUAL(slice.layout().name(), result.layout().name());
  CHECK_EQUAL(result.layout(), layout_after_delete);
  CHECK_EQUAL(slice.offset(), result.offset());
  self->send_exit(transformer, caf::exit_reason::user_shutdown);
}

FIXTURE_SCOPE_END()

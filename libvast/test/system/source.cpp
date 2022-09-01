//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#define SUITE source

#include "vast/system/source.hpp"

#include "vast/fwd.hpp"

#include "vast/detail/make_io_stream.hpp"
#include "vast/format/zeek.hpp"
#include "vast/table_slice.hpp"
#include "vast/test/data.hpp"
#include "vast/test/fixtures/actor_system_and_events.hpp"
#include "vast/test/test.hpp"

#include <caf/attach_stream_sink.hpp>

#include <optional>

using namespace vast;
using namespace vast::system;

namespace {

struct test_sink_state {
  std::vector<table_slice> slices;
  inline static constexpr const char* name = "test-sink";
};

stream_sink_actor<table_slice, std::string>::behavior_type test_sink(
  stream_sink_actor<table_slice, std::string>::stateful_pointer<test_sink_state>
    self,
  caf::actor src) {
  self->anon_send(
    src, static_cast<stream_sink_actor<table_slice, std::string>>(self));
  return {
    [=](caf::stream<table_slice> in,
        const std::string&) -> caf::inbound_stream_slot<table_slice> {
      return caf::attach_stream_sink(
               self, in,
               [](caf::unit_t&) {
                 // nop
               },
               [=](caf::unit_t&, table_slice slice) {
                 self->state.slices.emplace_back(std::move(slice));
               },
               [=](caf::unit_t&, const caf::error&) {
                 MESSAGE(self->name() << " is done");
               })
        .inbound_slot();
    },
  };
}

class fixture : public fixtures::deterministic_actor_system_and_events {
public:
  fixture()
    : fixtures::deterministic_actor_system_and_events(
      VAST_PP_STRINGIFY(SUITE)) {
  }
};

} // namespace

FIXTURE_SCOPE(source_tests, fixture)

TEST(zeek source) {
  MESSAGE("start reader");
  auto stream
    = unbox(detail::make_input_stream(artifacts::logs::zeek::small_conn));
  auto reader = std::make_unique<format::zeek::reader>(caf::settings{},
                                                       std::move(stream));
  MESSAGE("start source for producing table slices of size 10");
  auto src
    = self->spawn(source, std::move(reader), events::slice_size, std::nullopt,
                  vast::system::type_registry_actor{}, vast::module{},
                  std::string{}, vast::system::accountant_actor{},
                  std::vector<vast::pipeline>{});
  run();
  MESSAGE("start sink and run exhaustively");
  auto snk = self->spawn(test_sink, src);
  run();
  MESSAGE("get slices");
  const auto& slices
    = deref<stream_sink_actor<table_slice,
                              std::string>::stateful_base<test_sink_state>>(snk)
        .state.slices;
  MESSAGE("compare slices to auto-generates ones");
  REQUIRE_EQUAL(slices.size(), zeek_conn_log.size());
  for (size_t i = 0; i < slices.size(); ++i)
    CHECK_EQUAL(slices[i], zeek_conn_log[i]);
  MESSAGE("shutdown");
  self->send_exit(src, caf::exit_reason::user_shutdown);
  run();
}

FIXTURE_SCOPE_END()

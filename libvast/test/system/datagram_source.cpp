//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#define SUITE datagram_source

#include "vast/system/datagram_source.hpp"

#include "vast/format/zeek.hpp"
#include "vast/test/data.hpp"
#include "vast/test/fixtures/actor_system_and_events.hpp"
#include "vast/test/test.hpp"

#include <caf/exit_reason.hpp>
#include <caf/io/middleman.hpp>
#include <caf/send.hpp>

#include <algorithm>
#include <fstream>
#include <iterator>
#include <optional>

using namespace vast;
using namespace vast::system;

namespace {

struct test_sink_state {
  std::vector<table_slice> slices;
  inline static constexpr const char* name = "test-sink";
};

using test_sink_actor
  = caf::typed_actor<caf::reacts_to<atom::ping>>::extend_with<
    stream_sink_actor<stream_controlled<table_slice>, std::string>>;

test_sink_actor::behavior_type
test_sink(test_sink_actor::stateful_pointer<test_sink_state> self,
          const caf::actor& src) {
  self->anon_send(
    src,
    static_cast<stream_sink_actor<stream_controlled<table_slice>, std::string>>(
      self));
  return {
    [=](caf::stream<stream_controlled<table_slice>> in, const std::string&)
      -> caf::inbound_stream_slot<stream_controlled<table_slice>> {
      auto result = self->make_sink(
        in,
        [](caf::unit_t&) {
          // nop
        },
        [=](caf::unit_t&, stream_controlled<table_slice> x) {
          REQUIRE(caf::holds_alternative<table_slice>(x));
          auto& slice = caf::get<table_slice>(x);
          self->state.slices.emplace_back(std::move(slice));
        },
        [=](caf::unit_t&, const caf::error&) {
          CAF_MESSAGE(self->name() << " is done");
        });
      return result.inbound_slot();
    },
    [=](atom::ping) {
      REQUIRE_EQUAL(self->state.slices.size(), 1u);
      CHECK_EQUAL(self->state.slices.front().rows(), 20u);
    },
  };
}

} // namespace

FIXTURE_SCOPE(source_tests, fixtures::deterministic_actor_system_and_events)

TEST(zeek conn source) {
  MESSAGE("start source for producing table slices of size 100");
  auto stream = std::make_unique<std::istringstream>("wrong input");
  auto reader = std::make_unique<format::zeek::reader>(caf::settings{},
                                                       std::move(stream));
  auto hdl = caf::io::datagram_handle::from_int(1);
  auto& mm = sys.middleman();
  mpx.provide_datagram_servant(8080, hdl);
  auto src = mm.spawn_broker(datagram_source, uint16_t{8080}, std::move(reader),
                             100u, std::nullopt, type_registry_actor{},
                             vast::schema{}, std::string{}, accountant_actor{},
                             std::vector<transform>{});
  run();
  MESSAGE("start sink and initialize stream");
  auto snk = self->spawn(test_sink, src);
  REQUIRE(snk);
  run();
  MESSAGE("'send' datagram to src with a small Zeek conn log");
  caf::io::new_datagram_msg msg;
  msg.handle = caf::io::datagram_handle::from_int(2);
  using iter = std::istreambuf_iterator<char>;
  std::ifstream in{artifacts::logs::zeek::small_conn};
  REQUIRE(in.good());
  iter first{in};
  iter last{};
  for (; first != last; ++first)
    msg.buf.push_back(*first);
  caf::anon_send(src, std::move(msg));
  MESSAGE("advance streams and verify results");
  run();
  caf::anon_send(snk, atom::ping_v);
  run();
  caf::anon_send_exit(src, caf::exit_reason::user_shutdown);
  run();
}

FIXTURE_SCOPE_END()

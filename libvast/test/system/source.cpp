/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#define SUITE source

#include "vast/system/source.hpp"

#include "vast/test/data.hpp"
#include "vast/test/fixtures/actor_system_and_events.hpp"
#include "vast/test/test.hpp"

#include "vast/detail/make_io_stream.hpp"
#include "vast/format/zeek.hpp"
#include "vast/fwd.hpp"
#include "vast/table_slice.hpp"

using namespace vast;
using namespace vast::system;

namespace {

struct test_sink_state {
  std::vector<table_slice> slices;
  inline static constexpr const char* name = "test-sink";
};

using test_sink_type = caf::stateful_actor<test_sink_state>;

caf::behavior test_sink(test_sink_type* self, caf::actor src) {
  self->send(src, atom::sink_v, self);
  return {[=](caf::stream<table_slice> in, const std::string&) {
    return self->make_sink(
      in,
      [](caf::unit_t&) {
        // nop
      },
      [=](caf::unit_t&, table_slice slice) {
        self->state.slices.emplace_back(std::move(slice));
      },
      [=](caf::unit_t&, const caf::error&) {
        MESSAGE(self->name() << " is done");
      });
  }};
}

} // namespace <anonymous>

FIXTURE_SCOPE(source_tests, fixtures::deterministic_actor_system_and_events)

TEST(zeek source) {
  MESSAGE("start reader");
  auto stream = unbox(
    detail::make_input_stream(artifacts::logs::zeek::small_conn));
  format::zeek::reader reader{caf::settings{}, std::move(stream)};
  MESSAGE("start source for producing table slices of size 10");
  auto src = self->spawn(source<format::zeek::reader>, std::move(reader),
                         events::slice_size, caf::none,
                         vast::system::type_registry_actor{}, vast::schema{},
                         std::string{}, vast::system::accountant_actor{});
  run();
  MESSAGE("start sink and run exhaustively");
  auto snk = self->spawn(test_sink, src);
  run();
  MESSAGE("get slices");
  const auto& slices = deref<test_sink_type>(snk).state.slices;
  MESSAGE("compare slices to auto-generates ones");
  REQUIRE_EQUAL(slices.size(), zeek_conn_log.size());
  for (size_t i = 0; i < slices.size(); ++i)
    CHECK_EQUAL(slices[i], zeek_conn_log[i]);
  MESSAGE("shutdown");
  self->send_exit(src, caf::exit_reason::user_shutdown);
  run();
}

FIXTURE_SCOPE_END()

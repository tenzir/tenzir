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
#include "vast/test/test.hpp"

#include "vast/test/fixtures/actor_system_and_events.hpp"

#include "vast/detail/make_io_stream.hpp"
#include "vast/format/bro.hpp"
#include "vast/system/atoms.hpp"
#include "vast/table_slice.hpp"
#include "vast/subset.hpp"

using namespace vast;
using namespace vast::system;

namespace {

struct test_sink_state {
  std::vector<table_slice_ptr> slices;
  inline static constexpr const char* name = "test-sink";
};

using test_sink_type = caf::stateful_actor<test_sink_state>;

caf::behavior test_sink(test_sink_type* self, caf::actor src) {
  self->send(src, sink_atom::value, self);
  return {
    [=](caf::stream<table_slice_ptr> in) {
      return self->make_sink(
        in,
        [](caf::unit_t&) {
          // nop
        },
        [=](caf::unit_t&, table_slice_ptr ptr) {
          self->state.slices.emplace_back(std::move(ptr));
        },
        [=](caf::unit_t&, const error&) {
          CAF_MESSAGE(self->name() << " is done");
        }
      );
    }
  };
}

} // namespace <anonymous>

FIXTURE_SCOPE(source_tests, fixtures::deterministic_actor_system_and_events)

TEST(bro source) {
  MESSAGE("start reader");
  namespace bf = format::bro;
  auto stream = detail::make_input_stream(bro::small_conn);
  REQUIRE(stream);
  bf::reader reader{std::move(*stream)};
  MESSAGE("start source for producing table slices of size 10");
  auto src = self->spawn(source<bf::reader>, std::move(reader),
                         default_table_slice_builder::make,
                         events::slice_size);
  run();
  MESSAGE("start sink and run exhaustively");
  auto snk = self->spawn(test_sink, src);
  run();
  MESSAGE("get slices");
  const auto& slices = deref<test_sink_type>(snk).state.slices;
  MESSAGE("collect all rows as values");
  REQUIRE_EQUAL(slices.size(), 3u);
  std::vector<value> row_contents;
  for (size_t row = 0; row < 3u; ++row) {
    /// The first column is the automagically added timestamp.
    auto xs = subset(*slices[row], 0, table_slice::npos, 1);
    std::move(xs.begin(), xs.end(), std::back_inserter(row_contents));
  }
  std::vector<value> bro_conn_log_values;
  for (auto& x : bro_conn_log)
    bro_conn_log_values.emplace_back(flatten(x));
  REQUIRE_EQUAL(row_contents.size(), bro_conn_log_values.size());
  for (size_t i = 0; i < row_contents.size(); ++i)
    REQUIRE_EQUAL(row_contents[i], bro_conn_log_values[i]);
  MESSAGE("compare slices to auto-generates ones");
  REQUIRE_EQUAL(slices.size(), bro_conn_log_slices.size());
  for (size_t i = 0; i < slices.size(); ++i)
    CHECK_EQUAL(*slices[i], *bro_conn_log_slices[i]);
  MESSAGE("shutdown");
  self->send_exit(src, caf::exit_reason::user_shutdown);
  run();
}

FIXTURE_SCOPE_END()

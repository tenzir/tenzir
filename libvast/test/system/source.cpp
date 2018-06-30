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

#include "data.hpp"
#include "test.hpp"

#include "fixtures/actor_system_and_events.hpp"

#include "vast/detail/make_io_stream.hpp"
#include "vast/format/bro.hpp"
#include "vast/system/atoms.hpp"
#include "vast/table_slice.hpp"

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
  auto stream = detail::make_input_stream(bro::conn);
  REQUIRE(stream);
  bf::reader reader{std::move(*stream)};
  MESSAGE("start source for producing table slices of size 100");
  auto src = self->spawn(source<bf::reader>, std::move(reader),
                         default_table_slice::make_builder,
                         100u);
  sched.run();
  MESSAGE("start sink and run exhaustively");
  auto snk = self->spawn(test_sink, src);
  run_exhaustively();
  MESSAGE("collect all rows as values");
  auto& st = deref<test_sink_type>(snk).state;
  REQUIRE_EQUAL(st.slices.size(), 85u);
  std::vector<value> row_contents;
  for (size_t row = 0; row < 85u; ++row) {
    /// The first column is the automagically added timestamp.
    auto xs = st.slices[row]->rows_to_values(0, table_slice::npos, 1);
    std::move(xs.begin(), xs.end(), std::back_inserter(row_contents));
  }
  std::vector<value> bro_conn_log_values;
  for (auto& x : bro_conn_log)
    bro_conn_log_values.emplace_back(flatten(x));
  REQUIRE_EQUAL(row_contents.size(), bro_conn_log_values.size());
  for (size_t i = 0; i < row_contents.size(); ++i)
    REQUIRE_EQUAL(row_contents[i], bro_conn_log_values[i]);
  MESSAGE("shutdown");
  self->send_exit(src, caf::exit_reason::user_shutdown);
  sched.run();
}

FIXTURE_SCOPE_END()

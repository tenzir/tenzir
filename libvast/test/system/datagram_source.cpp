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

#define SUITE datagram_source

#include "vast/system/datagram_source.hpp"

#include "vast/test/test.hpp"
#include "vast/test/data.hpp"

#include "vast/test/fixtures/actor_system_and_events.hpp"

#include <algorithm>
#include <fstream>
#include <iterator>

#include <caf/exit_reason.hpp>
#include <caf/io/middleman.hpp>
#include <caf/send.hpp>

#include "vast/format/zeek.hpp"

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

TEST(zeek conn source) {
  MESSAGE("start source for producing table slices of size 100");
  namespace bf = format::zeek;
  bf::reader reader{defaults::system::table_slice_type};
  auto hdl = caf::io::datagram_handle::from_int(1);
  auto& mm = sys.middleman();
  mpx.provide_datagram_servant(8080, hdl);
  auto src = mm.spawn_broker(datagram_source<bf::reader>, uint16_t{8080},
                             std::move(reader),
                             default_table_slice_builder::make, 100u,
                             caf::none);
  run();
  MESSAGE("start sink and initialize stream");
  auto snk = self->spawn(test_sink, src);
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
  anon_send(src, std::move(msg));
  MESSAGE("advance streams and verify results");
  run();
  auto& st = deref<test_sink_type>(snk).state;
  REQUIRE_EQUAL(st.slices.size(), 1u);
  CHECK_EQUAL(st.slices.front()->rows(), 20u);
  anon_send_exit(src, caf::exit_reason::user_shutdown);
  run();
}

FIXTURE_SCOPE_END()

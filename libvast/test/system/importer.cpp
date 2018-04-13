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

#include "vast/concept/printable/stream.hpp"
#include "vast/concept/printable/vast/event.hpp"

#include "vast/system/archive.hpp"
#include "vast/system/data_store.hpp"
#include "vast/system/importer.hpp"

#include "vast/detail/spawn_container_source.hpp"

#define SUITE import
#include "test.hpp"
#include "fixtures/actor_system_and_events.hpp"

#include <caf/test/dsl.hpp>

using std::string;

using namespace caf;
using namespace vast;

namespace {

struct config : fixtures::actor_system::configuration {
  config() {
    // Reset log filter and unload all modules.
    logger_component_filter.clear();
    module_factories.clear();
  }
};

struct fixture : test_coordinator_fixture<config>,
                 fixtures::events,
                 fixtures::filesystem {
  // nop
};

} // namespace <anonymous>

FIXTURE_SCOPE(import_tests, fixture)

TEST(importer) {
  MESSAGE("spawn importer + store");
  directory /= "importer";
  auto importer = self->spawn(system::importer, directory);
  auto store = self->spawn(system::data_store<std::string, data>);
  self->send(importer, store);
  CAF_MESSAGE("run initialization code");
  sched.run();
  MESSAGE("spawn dummy source");
  auto src = vast::detail::spawn_container_source(self->system(), importer, bro_conn_log);
  sched.run_once();
  MESSAGE("expect the importer to give 0 initial credit");
  expect((open_stream_msg), from(src).to(importer));
  expect((upstream_msg::ack_open), from(importer).to(src).with(_, _, 0, _));
  MESSAGE("expect the importer to fetch more IDs");
  expect((atom_value, string, data), from(importer).to(store).with(_, _, _));
  expect((data), from(store).to(importer));
  MESSAGE("loop until the source is done sending");
  bool done_sending = false;
  while (!done_sending) {
    if (allow((upstream_msg::ack_batch), from(importer).to(src))) {
      // The importer may have granted more credit. The source responds with
      // batches.
      while (allow((downstream_msg::batch), from(src).to(importer))) {
        // Loop until all batches are handled.
      }
      // Check whether the source is done.
      if  (allow((downstream_msg::close), from(src).to(importer))) {
        MESSAGE("source is done sending");
        done_sending = true;
      }
    } else {
      // Check whether the importer can still produce more credit.
      MESSAGE("trigger timeouts for next credit round");
      sched.clock().current_time += credit_round_interval;
      sched.dispatch();
      allow((timeout_msg), from(src).to(src));
      expect((timeout_msg), from(importer).to(importer));
      if (!received<upstream_msg>(src)) {
        // No credit was generated, this means the importer lacks IDs.
        expect((atom_value, string, data), from(importer).to(store).with(_, _, _));
        expect((data), from(store).to(importer));
      }
    }
  }
  self->send_exit(importer, exit_reason::user_shutdown);
  sched.run();
}

FIXTURE_SCOPE_END()

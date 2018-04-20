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

using event_buffer = std::vector<event>;
using shared_event_buffer = std::shared_ptr<event_buffer>;

behavior dummy_sink(event_based_actor* self, shared_event_buffer buf) {
  return {
    [=](stream<event> in) {
      self->make_sink(
        in,
        [=](unit_t&) {
          // nop
        },
        [=](unit_t&, event x) {
          buf->push_back(std::move(x));
        }
      );
    }
  };
}

struct fixture : fixtures::deterministic_actor_system_and_events {
  fixture() {
    MESSAGE("spawn importer + store");
    directory /= "importer";
    importer = self->spawn(system::importer, directory);
    store = self->spawn(system::data_store<std::string, data>);
    self->send(importer, store);
    MESSAGE("run initialization code");
    sched.run();
  }

  ~fixture() {
    anon_send_exit(importer, exit_reason::kill);
  }

  actor importer;
  system::key_value_store_type<string, data> store;
};

} // namespace <anonymous>

FIXTURE_SCOPE(import_tests, fixture)

TEST(import without subscribers) {
  MESSAGE("spawn dummy source");
  auto src = vast::detail::spawn_container_source(self->system(), importer,
                                                  bro_conn_log);
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
        expect((atom_value, string, data),
               from(importer).to(store).with(_, _, _));
        expect((data), from(store).to(importer));
      }
    }
  }
}

TEST(import with one subscriber) {
  MESSAGE("spawn dummy sink");
  auto buf = std::make_shared<std::vector<event>>();
  auto subscriber = self->spawn(dummy_sink, buf);
  MESSAGE("connect sink to importer");
  anon_send(importer, add_atom::value, subscriber);
  sched.run();
  MESSAGE("spawn dummy source");
  auto src = vast::detail::spawn_container_source(self->system(), importer,
                                                  bro_conn_log);
  sched.run_once();
  MESSAGE("loop until importer becomes idle");
  sched.run_dispatch_loop(credit_round_interval);
  MESSAGE("check whether the sink received all events");
  CHECK_EQUAL(buf->size(), bro_conn_log.size());
}

TEST(import with two subscribers) {
  MESSAGE("spawn dummy sinks");
  auto buf1 = std::make_shared<std::vector<event>>();
  auto subscriber1 = self->spawn(dummy_sink, buf1);
  auto buf2 = std::make_shared<std::vector<event>>();
  auto subscriber2 = self->spawn(dummy_sink, buf2);
  MESSAGE("connect sinks to importer");
  anon_send(importer, add_atom::value, subscriber1);
  anon_send(importer, add_atom::value, subscriber2);
  sched.run();
  MESSAGE("spawn dummy source");
  auto src = vast::detail::spawn_container_source(self->system(), importer,
                                                  bro_conn_log);
  sched.run_once();
  MESSAGE("loop until importer becomes idle");
  sched.run_dispatch_loop(credit_round_interval);
  MESSAGE("check whether both sinks received all events");
  CHECK_EQUAL(*buf1, *buf2);
  CHECK_EQUAL(buf1->size(), bro_conn_log.size());
  CHECK_EQUAL(buf2->size(), bro_conn_log.size());
}

FIXTURE_SCOPE_END()

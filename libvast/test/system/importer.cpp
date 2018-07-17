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
#include "vast/const_table_slice_handle.hpp"
#include "vast/detail/spawn_container_source.hpp"
#include "vast/event.hpp"
#include "vast/system/archive.hpp"
#include "vast/system/data_store.hpp"
#include "vast/system/importer.hpp"
#include "vast/table_slice.hpp"
#include "vast/table_slice_handle.hpp"

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
    [=](stream<const_table_slice_handle> in) {
      self->make_sink(
        in,
        [=](unit_t&) {
          // nop
        },
        [=](unit_t&, const_table_slice_handle x) {
          using caf::get;
          auto event_id = x->offset();
          for (size_t row = 0; row < x->rows(); ++row) {
            // Get the current row, skipping the timestamp.
            auto row_data = unbox(x->row_to_value(row, 1));
            // Get only the timestamp.
            auto tstamp = unbox(x->row_to_value(row, 0, 1));
            // Convert the row content back to an event.
            auto e = event::make(row_data);
            e.timestamp(get<timestamp>(get<vector>(tstamp.get_data()).front()));
            e.id(event_id);
            ++event_id;
            buf->push_back(std::move(e));
          }
        }
      );
    }
  };
}

struct fixture : fixtures::deterministic_actor_system_and_events {
  fixture() {
    MESSAGE("spawn importer + store");
    directory /= "importer";
    importer = self->spawn(system::importer, directory, slice_size);
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

TEST(import with one subscriber) {
  MESSAGE("spawn dummy sink");
  auto buf = std::make_shared<std::vector<event>>();
  auto subscriber = self->spawn(dummy_sink, buf);
  MESSAGE("connect sink to importer");
  anon_send(importer, add_atom::value, subscriber);
  sched.run();
  MESSAGE("spawn dummy source");
  auto src = vast::detail::spawn_container_source(self->system(),
                                                  bro_conn_log_slices,
                                                  importer);
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
  auto src = vast::detail::spawn_container_source(self->system(),
                                                  bro_conn_log_slices,
                                                  importer);
  sched.run_once();
  MESSAGE("loop until importer becomes idle");
  sched.run_dispatch_loop(credit_round_interval);
  MESSAGE("check whether both sinks received all events");
  CHECK_EQUAL(*buf1, *buf2);
  CHECK_EQUAL(buf1->size(), bro_conn_log.size());
  CHECK_EQUAL(buf2->size(), bro_conn_log.size());
}

FIXTURE_SCOPE_END()

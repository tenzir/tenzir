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

#define SUITE indexer_stage_driver
#include "test.hpp"

#include <random>
#include <vector>

#include "vast/system/indexer_stage_driver.hpp"

#include "fixtures/actor_system_and_events.hpp"

#include "vast/detail/spawn_container_source.hpp"

using namespace caf;
using namespace vast;
using namespace vast::system;

namespace {

using event_buffer = std::vector<event>;

using shared_event_buffer = std::shared_ptr<event_buffer>;

behavior dummy_sink(event_based_actor* self, size_t* dummy_sink_count,
                    shared_event_buffer buf) {
  *dummy_sink_count += 1;
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

auto indexer_manager_factory(actor_system& sys, path p, size_t* dummy_count,
                             shared_event_buffer buf) {
  return [=, &sys] {
    auto sink_factory = [=, &sys](path, type t) -> actor {
      MESSAGE("spawn a new type for the type " << t);
      return sys.spawn(dummy_sink, dummy_count, buf);
    };
    return caf::make_counted<indexer_manager>(p, sink_factory);
  };
}

behavior test_stage(event_based_actor* self,
                    indexer_stage_driver::index_manager_factory f) {
  return {
    [=](stream<event> in) {
      auto mgr = self->make_continuous_stage<indexer_stage_driver>(f);
      mgr->add_inbound_path(in);
    }
  };
}

struct fixture : fixtures::deterministic_actor_system_and_events {
  fixture() {
    // Build a test data set with multiple event types.
    auto pick_some_from = [&](const std::vector<event>& xs, size_t amount) {
      REQUIRE(amount <= xs.size());
      auto first = xs.begin();
      auto last = first + amount;
      std::copy(first, last, std::back_inserter(test_events));
    };
    pick_some_from(bro_conn_log, 20);
    pick_some_from(bro_dns_log, 20);
    pick_some_from(bro_http_log, 20);
    pick_some_from(bgpdump_txt, 20);
    pick_some_from(random, 20);
    // TODO: Compute that number. Currently blocked by story 2587, because we
    //       can't sort our test events by type to get the number of event
    //       types via unique.
    num_types = 5;
    // Randomize data set.
    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(test_events.begin(), test_events.end(), g);
  }

  /// Makes sure no persistet state exists.
  void wipe_persisted_state() {
    rm(state_dir);
  }

  /// Directory where the manager is supposed to persist its state.
  path state_dir = directory / "indexer-manager";

  std::vector<event> test_events;

  size_t num_types;
};

} // namespace <anonymous>

FIXTURE_SCOPE(indexer_stage_driver_tests, fixture)

TEST(spawning sink automatically) {
  wipe_persisted_state();
  MESSAGE("spawn the stage");
  auto dummies = size_t{0};
  auto buf = std::make_shared<std::vector<event>>();
  auto stg = sys.spawn(test_stage,
                       indexer_manager_factory(sys, state_dir, &dummies, buf));
  MESSAGE("spawn the source");
  auto src = vast::detail::spawn_container_source(self->system(), stg, random);
  MESSAGE("run exhaustively");
  sched.run_dispatch_loop(credit_round_interval);
  CHECK_EQUAL(dummies, num_types);
  MESSAGE("check content of the shared buffer");
  CHECK_EQUAL(test_events.size(), buf->size());
  std::sort(test_events.begin(), test_events.end());
  std::sort(buf->begin(), buf->end());
  CHECK_EQUAL(test_events, *buf);
  anon_send_exit(stg, exit_reason::user_shutdown);
}

FIXTURE_SCOPE_END()

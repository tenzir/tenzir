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

#include "vast/meta_index.hpp"
#include "vast/detail/spawn_container_source.hpp"
#include "vast/system/indexer_stage_driver.hpp"
#include "vast/system/partition.hpp"
#include "vast/uuid.hpp"

#include "fixtures/actor_system_and_events.hpp"

using namespace caf;
using namespace vast;
using namespace vast::system;

using std::shared_ptr;
using std::make_shared;

namespace {

using event_buffer = std::vector<event>;

using shared_event_buffer = shared_ptr<event_buffer>;

using shared_event_buffer_vector = std::vector<shared_event_buffer>;

behavior dummy_sink(event_based_actor* self, size_t* dummy_sink_count,
                    shared_event_buffer buf) {
  *dummy_sink_count += 1;
  MESSAGE("initialize sink #" << *dummy_sink_count);
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
      self->unbecome();
    }
  };
}

auto partition_factory(actor_system& sys, path p, size_t* dummy_count,
                       shared_ptr<shared_event_buffer_vector> bufs) {
  return [=, &sys] {
    bufs->emplace_back(std::make_shared<event_buffer>());
    auto buf = bufs->back();
    auto sink_factory = [=, &sys](path, type t) -> actor {
      MESSAGE("spawn a new sink for type " << t);
      return sys.spawn(dummy_sink, dummy_count, buf);
    };
    auto id = uuid::random();
    return make_partition(p, std::move(id), sink_factory);
  };
}

behavior test_stage(event_based_actor* self, meta_index* pi,
                    indexer_stage_driver::partition_factory f, size_t mps) {
  return {[=](stream<event> in) {
    auto mgr = self->make_continuous_stage<indexer_stage_driver>(*pi, f, mps);
    mgr->add_inbound_path(in);
    self->unbecome();
  }};
}

struct fixture : fixtures::deterministic_actor_system_and_events {
  fixture() {
    /// Only needed for computing how many types are in our data set.
    std::set<type> types;
    /// Makes sure no persistet state exists.
    rm(state_dir);
    // Build a test data set with multiple event types.
    auto pick_from = [&](const std::vector<event>& xs, size_t index) {
      VAST_ASSERT(index < xs.size());
      auto& x = xs[index];
      test_events.emplace_back(x);
      types.emplace(x.type());
    };
    // Pick 100 events from various data sets in the worst-case distribution of
    // types.
    for (size_t i = 0; i < 20; ++i) {
      pick_from(bro_conn_log, i);
      pick_from(bro_dns_log, i);
      pick_from(bro_http_log, i);
      pick_from(bgpdump_txt, i);
      pick_from(random, i);
    }
    num_types = types.size();
  }

  /// Directory where the manager is supposed to persist its state.
  path state_dir = directory / "indexer-manager";

  meta_index pindex;

  std::vector<event> test_events;

  size_t num_types;
};

} // namespace <anonymous>

FIXTURE_SCOPE(indexer_stage_driver_tests, fixture)

TEST(spawning sinks automatically) {
  MESSAGE("spawn the stage");
  auto dummies = size_t{0};
  auto bufs = make_shared<shared_event_buffer_vector>();
  auto stg = sys.spawn(test_stage, &pindex,
                       partition_factory(sys, state_dir, &dummies, bufs),
                       std::numeric_limits<size_t>::max());
  MESSAGE("spawn the source and run");
  auto src = vast::detail::spawn_container_source(self->system(),
                                                  test_events, stg);
  run_exhaustively();
  CHECK_EQUAL(dummies, num_types);
  MESSAGE("check content of the shared buffer");
  REQUIRE_EQUAL(bufs->size(), 1u);
  auto& buf = bufs->back();
  CHECK_EQUAL(test_events.size(), buf->size());
  std::sort(test_events.begin(), test_events.end());
  std::sort(buf->begin(), buf->end());
  CHECK_EQUAL(test_events, *buf);
  anon_send_exit(stg, exit_reason::user_shutdown);
}

TEST(creating integer partitions automatically) {
  MESSAGE("spawn the stage");
  auto dummies = size_t{0};
  auto bufs = make_shared<shared_event_buffer_vector>();
  auto stg = sys.spawn(test_stage, &pindex,
                       partition_factory(sys, state_dir, &dummies, bufs),
                       10u);
  MESSAGE("spawn the source and run");
  auto src = vast::detail::spawn_container_source(self->system(),
                                                  test_events, stg);
  run_exhaustively();
  MESSAGE("11 segments must exist, 10 with 10 elements and one empty");
  CHECK_EQUAL(bufs->size(), 11u);
  CHECK(bufs->back()->empty());
  bufs->pop_back();
  event_buffer xs;
  MESSAGE("flatten all partitions into one buffer");
  for (auto& buf : *bufs) {
    xs.insert(xs.end(), buf->begin(), buf->end());
    CHECK_EQUAL(buf->size(), 10u);
  }
  CHECK_EQUAL(test_events.size(), xs.size());
  std::sort(xs.begin(), xs.end());
  std::sort(test_events.begin(), test_events.end());
  CHECK_EQUAL(xs, test_events);
  anon_send_exit(stg, exit_reason::user_shutdown);
}

TEST(creating bro conn log partitions automatically) {
  MESSAGE("spawn the stage");
  auto dummies = size_t{0};
  auto bufs = make_shared<shared_event_buffer_vector>();
  auto stg = sys.spawn(test_stage, &pindex,
                       partition_factory(sys, state_dir, &dummies, bufs),
                       100u);
  MESSAGE("spawn the source and run");
  auto src = vast::detail::spawn_container_source(self->system(),
                                                  bro_conn_log, stg);
  run_exhaustively();
  MESSAGE("flatten all partitions into one buffer");
  event_buffer xs;
  for (auto& buf : *bufs)
    xs.insert(xs.end(), buf->begin(), buf->end());
  CHECK_EQUAL(bro_conn_log.size(), xs.size());
  std::sort(xs.begin(), xs.end());
  auto ys = bro_conn_log;
  std::sort(ys.begin(), ys.end());
  CHECK_EQUAL(xs, ys);
  anon_send_exit(stg, exit_reason::user_shutdown);
}

FIXTURE_SCOPE_END()

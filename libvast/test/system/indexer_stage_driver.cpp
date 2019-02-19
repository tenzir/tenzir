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

#include "vast/system/indexer_stage_driver.hpp"

#include "vast/test/test.hpp"

#include "vast/test/fixtures/actor_system_and_events.hpp"

#include <random>
#include <vector>

#include "vast/concept/printable/to_string.hpp"
#include "vast/detail/spawn_container_source.hpp"
#include "vast/logger.hpp"
#include "vast/meta_index.hpp"
#include "vast/system/index.hpp"
#include "vast/system/partition.hpp"
#include "vast/to_events.hpp"
#include "vast/uuid.hpp"

using namespace caf;
using namespace vast;
using namespace vast::system;

using std::shared_ptr;
using std::make_shared;

namespace {

template <class Container>
auto sorted(Container xs) {
  std::sort(xs.begin(), xs.end());
  return xs;
}

thread_local std::vector<caf::actor> all_sinks;

thread_local std::set<table_slice_ptr> all_slices;

behavior dummy_sink(event_based_actor* self) {
  return {[=](stream<table_slice_ptr> in) {
    self->make_sink(in,
                    [=](unit_t&) {
                      // nop
                    },
                    [=](unit_t&, table_slice_ptr slice) {
                      all_slices.emplace(std::move(slice));
                    });
    self->unbecome();
  }};
}

caf::actor spawn_sink(caf::local_actor* self, path dir, type t, size_t,
                      caf::actor, uuid partition_id, atomic_measurement*) {
  VAST_UNUSED(dir, t, partition_id);
  VAST_TRACE(VAST_ARG(dir), VAST_ARG("t", t.name()), VAST_ARG(partition_id));
  auto result = self->spawn(dummy_sink);
  all_sinks.emplace_back(result);
  return result;
}

behavior dummy_index(stateful_actor<index_state>* self, path dir) {
  VAST_TRACE(VAST_ARG(dir));
  self->state.init(dir, std::numeric_limits<size_t>::max(), 10, 5);
  self->state.factory = spawn_sink;
  return {[=](stream<table_slice_ptr> in) {
    auto mgr = self->make_continuous_stage<indexer_stage_driver>(self);
    mgr->add_inbound_path(in);
    self->unbecome();
  }};
}

struct fixture : fixtures::deterministic_actor_system_and_events {
  fixture() : expected_sink_count(0) {
    // Only needed for computing how many layouts are in our data set.
    std::set<record_type> layouts;
    // Make sure no persistet state exists.
    rm(state_dir);
    // Make sure we have a clean slate.
    all_sinks.clear();
    all_slices.clear();
    // Pick slices from various data sets.
    auto pick_from = [&](const auto& slices) {
      VAST_ASSERT(slices.size() > 0);
      test_slices.emplace_back(slices[0]);
      layouts.emplace(slices[0]->layout());
    };
    pick_from(zeek_conn_log_slices);
    pick_from(ascending_integers_slices);
    // TODO: uncomment when resolving [ch3215]
    // pick_from(zeek_http_log_slices);
    // pick_from(bgpdump_txt_slices);
    // pick_from(random_slices);
    num_layouts = layouts.size();
    for (auto& layout : layouts)
      expected_sink_count += layout.fields.size();
    REQUIRE_EQUAL(test_slices.size(), num_layouts);
    index = sys.spawn(dummy_index, state_dir / "dummy-index");
  }

  ~fixture() {
    // Make sure we're not leaving stuff behind.
    for (auto& snk : all_sinks)
      anon_send_exit(snk, exit_reason::user_shutdown);
    all_sinks.clear();
  }

  // Directory where the manager is supposed to persist its state.
  path state_dir = directory / "indexer-manager";

  // Dummy acting as INDEX.
  actor index;

  // Randomly picked table slices from the events fixture.
  std::vector<table_slice_ptr> test_slices;

  // Keeps track how many layouts are in `test_slices`.
  size_t num_layouts;

  // Tells us how many INDEXER actors *should* get started.
  size_t expected_sink_count;

  // Convenience getter for accessing the state of our dummy INDEX.
  index_state* state() {
    return std::addressof(deref<stateful_actor<index_state>>(index).state);
  }
};

} // namespace <anonymous>

FIXTURE_SCOPE(indexer_stage_driver_tests, fixture)

TEST(spawning sinks automatically) {
  MESSAGE("spawn the source and run");
  auto src = vast::detail::spawn_container_source(self->system(), test_slices,
                                                  index);
  run();
  CHECK_EQUAL(all_sinks.size(), expected_sink_count);
  CHECK_EQUAL(sorted(test_slices), all_slices);
}

/*
TEST(creating zeek conn log partitions automatically) {
  MESSAGE("spawn the stage");
  auto stg = sys.spawn(test_stage, &pindex,
                       partition_factory(sys, state_dir, &partition_count),
                       slice_size);
  MESSAGE("spawn the source and run");
  auto src = vast::detail::spawn_container_source(self->system(),
                                                  zeek_conn_log_slices, stg);
  run();
  CHECK_EQUAL(bufs->size(), zeek_conn_log_slices.size());
  MESSAGE("flatten all partitions into one buffer");
  event_buffer xs;
  for (auto& buf : *bufs)
    xs.insert(xs.end(), buf->begin(), buf->end());
  CHECK_EQUAL(zeek_conn_log.size(), xs.size());
  std::sort(xs.begin(), xs.end());
  auto ys = zeek_conn_log;
  std::sort(ys.begin(), ys.end());
  REQUIRE_EQUAL(xs.size(), ys.size());
  for (size_t i = 0; i < xs.size(); ++i)
    CHECK_EQUAL(xs[i], flatten(ys[i]));
  anon_send_exit(stg, exit_reason::user_shutdown);
}
*/

FIXTURE_SCOPE_END()

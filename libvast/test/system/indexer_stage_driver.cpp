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

#include "vast/const_table_slice_handle.hpp"
#include "vast/detail/spawn_container_source.hpp"
#include "vast/meta_index.hpp"
#include "vast/system/indexer_stage_driver.hpp"
#include "vast/system/partition.hpp"
#include "vast/table_slice_handle.hpp"
#include "vast/uuid.hpp"

#include "vast/concept/printable/to_string.hpp"

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
  return {
    [=](stream<const_table_slice_handle> in) {
      self->make_sink(
        in,
        [=](unit_t&) {
          // nop
        },
        [=](unit_t&, const_table_slice_handle slice) {
          auto xs = slice->rows_to_events();
          for (auto& x : xs)
            buf->emplace_back(std::move(x));
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
    auto sink_factory = [=, &sys](path, type) -> actor {
      return sys.spawn(dummy_sink, dummy_count, buf);
    };
    auto id = uuid::random();
    return make_partition(p, std::move(id), sink_factory);
  };
}

behavior test_stage(event_based_actor* self, meta_index* pi,
                    indexer_stage_driver::partition_factory f, size_t mps) {
  return {[=](stream<const_table_slice_handle> in) {
    auto mgr = self->make_continuous_stage<indexer_stage_driver>(*pi, f, mps);
    mgr->add_inbound_path(in);
    self->unbecome();
  }};
}

struct fixture : fixtures::deterministic_actor_system_and_events {
  fixture() {
    /// Only needed for computing how many layouts are in our data set.
    std::set<record_type> layouts;
    /// Makes sure no persistet state exists.
    rm(state_dir);
    // Pick slices from various data sets.
    auto pick_from = [&](const auto& slices) {
      VAST_ASSERT(slices.size() > 0);
      test_slices.emplace_back(slices[0]);
      layouts.emplace(slices[0]->layout());
    };
    pick_from(bro_conn_log_slices);
    pick_from(ascending_integers_slices);
    /// TODO: uncomment when resolving [ch3215]
    /// pick_from(bro_http_log_slices);
    /// pick_from(bgpdump_txt_slices);
    /// pick_from(random_slices);
    num_layouts = layouts.size();
    REQUIRE_EQUAL(test_slices.size(), num_layouts);
  }

  /// Directory where the manager is supposed to persist its state.
  path state_dir = directory / "indexer-manager";

  meta_index pindex;

  std::vector<const_table_slice_handle> test_slices;

  size_t num_layouts;
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
                                                  test_slices, stg);
  run_exhaustively();
  CHECK_EQUAL(dummies, num_layouts);
  MESSAGE("check content of the shared buffer");
  REQUIRE_EQUAL(bufs->size(), 1u);
  auto& buf = bufs->back();
  CHECK_EQUAL(test_slices.size() * slice_size, buf->size());
  std::sort(test_slices.begin(), test_slices.end());
  std::sort(buf->begin(), buf->end());
  CHECK_EQUAL(test_slices, *buf);
  anon_send_exit(stg, exit_reason::user_shutdown);
}

TEST(creating bro conn log partitions automatically) {
  MESSAGE("spawn the stage");
  auto dummies = size_t{0};
  auto bufs = make_shared<shared_event_buffer_vector>();
  auto stg = sys.spawn(test_stage, &pindex,
                       partition_factory(sys, state_dir, &dummies, bufs),
                       slice_size);
  MESSAGE("spawn the source and run");
  auto src = vast::detail::spawn_container_source(self->system(),
                                                  const_bro_conn_log_slices,
                                                  stg);
  run_exhaustively();
  CHECK_EQUAL(bufs->size(), const_bro_conn_log_slices.size());
  MESSAGE("flatten all partitions into one buffer");
  event_buffer xs;
  for (auto& buf : *bufs)
    xs.insert(xs.end(), buf->begin(), buf->end());
  CHECK_EQUAL(bro_conn_log.size(), xs.size());
  std::sort(xs.begin(), xs.end());
  auto ys = bro_conn_log;
  std::sort(ys.begin(), ys.end());
  REQUIRE_EQUAL(xs.size(), ys.size());
  for (size_t i = 0; i < xs.size(); ++i)
    CHECK_EQUAL(xs[i], flatten(ys[i]));
  anon_send_exit(stg, exit_reason::user_shutdown);
}

FIXTURE_SCOPE_END()

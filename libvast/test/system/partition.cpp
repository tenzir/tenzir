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

#define SUITE partition
#include "test.hpp"

#include "vast/system/partition.hpp"

#include <caf/atom.hpp>
#include <caf/behavior.hpp>
#include <caf/event_based_actor.hpp>

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/type.hpp"
#include "vast/event.hpp"
#include "vast/ids.hpp"

#include "fixtures/actor_system_and_events.hpp"

using namespace vast;
using namespace vast::system;

namespace {

caf::behavior dummy_indexer(caf::event_based_actor*) {
  return {
    [](caf::ok_atom) {
      // nop
    }
  };
}

struct query_state {
  ids result;
  size_t expected = 0;
  size_t received = 0;
};

struct query_actor_state {
  static inline const char* name = "query-actor";
};

void query_actor(caf::stateful_actor<query_actor_state>* self, query_state* qs,
                 partition_ptr put, const expression& expr) {
  qs->expected = put->lookup_requests(self, expr, [=](const ids& sub_result) {
    qs->received += 1;
    qs->result |= sub_result;
  });
}

template <class T>
std::vector<std::string> sorted_strings(const std::vector<T>& xs) {
  std::vector<std::string> result;
  for (auto& x : xs)
    result.emplace_back(to_string(x));
  std::sort(result.begin(), result.end());
  return result;
}

struct fixture : fixtures::deterministic_actor_system_and_events {
  fixture() : types{string_type{}, address_type{}, pattern_type{}} {
    min_running_actors = sys.registry().running();
  }

  /// Creates a partition that spawns dummy INDEXER actors.
  partition_ptr make_dummy_partition() {
    auto f = [&](path, type) {
      return sys.spawn(dummy_indexer);
    };
    return vast::system::make_partition(state_dir, partition_id, f);
  }

  /// Creates a partition that spawns actual INDEXER actors.
  partition_ptr make_partition() {
    return vast::system::make_partition(self.ptr(), state_dir, partition_id);
  }

  /// Creates a partition that spawns actual INDEXER actors with custom ID.
  partition_ptr make_partition(uuid id) {
    return vast::system::make_partition(self.ptr(), state_dir, id);
  }

  /// Returns how many dummy INDEXER actors are currently running.
  size_t running_indexers() {
    return sys.registry().running() - min_running_actors;
  }

  /// Makes sure no persistet state exists.
  void wipe_persisted_state() {
    rm(state_dir);
  }

  ids query(std::string_view what) {
    query_state qs;
    sys.spawn(query_actor, &qs, put, unbox(to<expression>(what)));
    sched.run();
    CHECK_EQUAL(qs.expected, qs.received);
    return std::move(qs.result);
  }

  /// The partition-under-test.
  partition_ptr put;

  /// A vector with some event types for testing.
  std::vector<type> types;

  /// Directory where the manager is supposed to persist its state.
  path state_dir = directory / "indexer-manager";

  /// Number of actors that run before the manager spawns INDEXER actors.
  size_t min_running_actors;

  /// Some UUID for the partition.
  uuid partition_id = uuid::random();
};

} // namespace <anonymous>

FIXTURE_SCOPE(indexer_manager_tests, fixture)

TEST(shutdown indexers in destructor) {
  MESSAGE("start manager");
  put = make_dummy_partition();
  MESSAGE("add INDEXER actors");
  for (auto& x : types)
    put->manager().get_or_add(x);
  REQUIRE_EQUAL(running_indexers(), types.size());
  CHECK_EQUAL(sorted_strings(put->types()), sorted_strings(types));
  MESSAGE("stop manager (and INDEXER actors)");
  put.reset();
  sched.run();
  REQUIRE_EQUAL(running_indexers(), 0u);
}

TEST(restore from meta data) {
  MESSAGE("start first manager");
  wipe_persisted_state();
  put = make_dummy_partition();
  REQUIRE_EQUAL(put->dirty(), false);
  MESSAGE("add INDEXER actors to first manager");
  for (auto& x : types)
    put->manager().get_or_add(x);
  REQUIRE_EQUAL(put->dirty(), true);
  REQUIRE_EQUAL(running_indexers(), types.size());
  CHECK_EQUAL(sorted_strings(put->types()), sorted_strings(types));
  MESSAGE("stop first manager");
  put.reset();
  sched.run();
  REQUIRE_EQUAL(running_indexers(), 0u);
  MESSAGE("start second manager and expect it to restore its persisted state");
  put = make_dummy_partition();
  REQUIRE_EQUAL(put->dirty(), false);
  REQUIRE_EQUAL(sorted_strings(put->types()), sorted_strings(types));
  put.reset();
  sched.run();
}

TEST(integer rows lookup) {
  MESSAGE("generate partition for flat integer type");
  put = make_partition();
  MESSAGE("ingest test data (integers)");
  integer_type layout;
  std::vector<int> ints{1, 2, 3, 1, 2, 3, 1, 2, 3};
  std::vector<event> events;
  for (auto i : ints)
    events.emplace_back(event::make(i, layout, events.size()));
  auto res = [&](auto... args) {
    return make_ids({args...}, events.size());
  };
  anon_send(put->manager().get_or_add(layout).first, events);
  sched.run();
  MESSAGE("verify partition content");
  CHECK_EQUAL(query(":int == +1"), res(0, 3, 6));
  CHECK_EQUAL(query(":int == +2"), res(1, 4, 7));
  CHECK_EQUAL(query(":int == +3"), res(2, 5, 8));
  CHECK_EQUAL(query(":int == +4"), res());
  CHECK_EQUAL(query(":int != +1"), res(1, 2, 4, 5, 7, 8));
  CHECK_EQUAL(query("!(:int == +1)"), res(1, 2, 4, 5, 7, 8));
  CHECK_EQUAL(query(":int > +1 && :int < +3"), res(1, 4, 7));
}

TEST(bro conn log lookup) {
  MESSAGE("generate partiton for bro conn log");
  put = make_partition();
  MESSAGE("ingest bro conn logs");
  auto layout = bro_conn_log[0].type();
  anon_send(put->manager().get_or_add(layout).first, bro_conn_log);
  sched.run();
  MESSAGE("verify partition content");
  auto res = [&](auto... args) {
    return make_ids({args...}, bro_conn_log.size());
  };
  CHECK_EQUAL(query(":addr == 169.254.225.22"), res(680, 682, 719, 720));
}

TEST(chunked bro conn log lookup) {
  static constexpr size_t chunk_size = 100;
  MESSAGE("ingest bro conn logs into partitions of size " << chunk_size);
  std::vector<partition_ptr> partitions;
  auto layout = bro_conn_log[0].type();
  for (size_t i = 0; i < bro_conn_log.size(); i += chunk_size) {
    std::vector<event> chunk;
    MESSAGE("ingest bro conn chunk nr. " << ((i / chunk_size) + 1));
    for (size_t j = i; j < std::min(i + chunk_size, bro_conn_log.size()); ++j)
      chunk.emplace_back(bro_conn_log[j]);
    auto ptr = make_partition(uuid::random());
    anon_send(ptr->manager().get_or_add(layout).first, std::move(chunk));
    partitions.emplace_back(std::move(ptr));
  }
  sched.run();
  MESSAGE("verify partition content");
  auto res = [&](auto... args) {
    return make_ids({args...}, bro_conn_log.size());
  };
  auto expr = unbox(to<expression>(":addr == 169.254.225.22"));
  ids result;
  for (auto& ptr : partitions) {
    query_state qs;
    sys.spawn(query_actor, &qs, ptr, expr);
    sched.run();
    CHECK_EQUAL(qs.expected, qs.received);
    result |= qs.result;
  }
  CHECK_EQUAL(rank(result), 4u);
  CHECK_EQUAL(result, res(680, 682, 719, 720));
}

FIXTURE_SCOPE_END()

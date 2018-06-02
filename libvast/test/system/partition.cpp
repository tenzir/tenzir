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

#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/type.hpp"

#include "fixtures/actor_system.hpp"

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

template <class T>
std::vector<std::string> sorted_strings(const std::vector<T>& xs) {
  std::vector<std::string> result;
  for (auto& x : xs)
    result.emplace_back(to_string(x));
  std::sort(result.begin(), result.end());
  return result;
}

struct fixture : fixtures::deterministic_actor_system {
  fixture() : types{string_type{}, address_type{}, pattern_type{}} {
    min_running_actors = sys.registry().running();
  }

  /// Creates an indexer manager that spawns dummy INDEXER actors.
  partition_ptr make_partition() {
    auto f = [&](path, type) {
      return sys.spawn(dummy_indexer);
    };
    return vast::system::make_partition(state_dir, partition_id, f);
  }

  /// Returns how many dummy INDEXER actors are currently running.
  size_t running_indexers() {
    return sys.registry().running() - min_running_actors;
  }

  /// Makes sure no persistet state exists.
  void wipe_persisted_state() {
    rm(state_dir);
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
  put = make_partition();
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
  put = make_partition();
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
  put = make_partition();
  REQUIRE_EQUAL(put->dirty(), false);
  REQUIRE_EQUAL(sorted_strings(put->types()), sorted_strings(types));
  put.reset();
  sched.run();
}

FIXTURE_SCOPE_END()

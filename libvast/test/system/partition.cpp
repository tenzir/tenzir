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
#include "vast/const_table_slice_handle.hpp"
#include "vast/default_table_slice.hpp"
#include "vast/detail/overload.hpp"
#include "vast/detail/spawn_container_source.hpp"
#include "vast/event.hpp"
#include "vast/ids.hpp"
#include "vast/system/indexer.hpp"
#include "vast/table_slice.hpp"
#include "vast/table_slice_handle.hpp"

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
  template <class T>
  static record_type rec() {
    return {{"value", T{}}};
  }

  fixture()
    : layouts{rec<string_type>(), rec<address_type>(), rec<pattern_type>()} {
    min_running_actors = sys.registry().running();
  }

  /// Creates a partition that spawns dummy INDEXER actors.
  partition_ptr make_dummy_partition() {
    auto f = [&](path, type) {
      return sys.spawn(dummy_indexer);
    };
    return vast::system::make_partition(sys, state_dir, partition_id, f);
  }

  /// Creates a partition that spawns actual INDEXER actors.
  partition_ptr make_partition() {
    return vast::system::make_partition(sys, self.ptr(), state_dir,
                                        partition_id);
  }

  /// Creates a partition that spawns actual INDEXER actors with custom ID.
  partition_ptr make_partition(uuid id) {
    return vast::system::make_partition(sys, self.ptr(), state_dir, id);
  }

  /// @returns how many dummy INDEXER actors are currently running.
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
    run();
    CHECK_EQUAL(qs.expected, qs.received);
    return std::move(qs.result);
  }

  /// The partition-under-test.
  partition_ptr put;

  /// A vector with some layouts for testing.
  std::vector<record_type> layouts;

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
  for (auto& x : layouts)
    put->manager().get_or_add(x);
  REQUIRE_EQUAL(running_indexers(), layouts.size());
  CHECK_EQUAL(sorted_strings(put->layouts()), sorted_strings(layouts));
  MESSAGE("stop manager (and INDEXER actors)");
  put.reset();
  run();
  REQUIRE_EQUAL(running_indexers(), 0u);
}

TEST(restore from meta data) {
  MESSAGE("start first manager");
  wipe_persisted_state();
  put = make_dummy_partition();
  REQUIRE_EQUAL(put->dirty(), false);
  MESSAGE("add INDEXER actors to first manager");
  for (auto& x : layouts)
    put->manager().get_or_add(x);
  REQUIRE_EQUAL(put->dirty(), true);
  REQUIRE_EQUAL(running_indexers(), layouts.size());
  CHECK_EQUAL(sorted_strings(put->layouts()), sorted_strings(layouts));
  MESSAGE("stop first manager");
  put.reset();
  run();
  REQUIRE_EQUAL(running_indexers(), 0u);
  MESSAGE("start second manager and expect it to restore its persisted state");
  put = make_dummy_partition();
  REQUIRE_EQUAL(put->dirty(), false);
  REQUIRE_EQUAL(sorted_strings(put->layouts()), sorted_strings(layouts));
  put.reset();
  run();
}

TEST(integer rows lookup) {
  MESSAGE("generate partition for flat integer type");
  put = make_partition();
  MESSAGE("ingest test data (integers)");
  integer_type col_type;
  record_type layout{{"value", col_type}};
  auto rows = make_rows(1, 2, 3, 1, 2, 3, 1, 2, 3);
  auto slice = default_table_slice::make(layout, rows);
  std::vector<const_table_slice_handle> slices{slice};
  detail::spawn_container_source(sys, std::move(slices),
                                 put->manager().get_or_add(layout).first);
  run();
  MESSAGE("verify partition content");
  auto res = [&](auto... args) {
    return make_ids({args...}, rows.size());
  };
  CHECK_EQUAL(query(":int == +1"), res(0u, 3u, 6u));
  CHECK_EQUAL(query(":int == +2"), res(1u, 4u, 7u));
  CHECK_EQUAL(query("value == +3"), res(2u, 5u, 8u));
  CHECK_EQUAL(query(":int == +4"), res());
  CHECK_EQUAL(query(":int != +1"), res(1u, 2u, 4u, 5u, 7u, 8u));
  CHECK_EQUAL(query("!(:int == +1)"), res(1u, 2u, 4u, 5u, 7u, 8u));
  CHECK_EQUAL(query(":int > +1 && :int < +3"), res(1u, 4u, 7u));
}

TEST(single partition bro conn log lookup) {
  MESSAGE("generate partiton for bro conn log");
  put = make_partition();
  MESSAGE("ingest bro conn logs");
  auto layout = bro_conn_log_layout();
  auto indexer = put->manager().get_or_add(layout).first;
  detail::spawn_container_source(sys, const_bro_conn_log_slices, indexer);
  run();
  MESSAGE("verify partition content");
  auto res = [&](auto... args) {
    return make_ids({args...}, bro_conn_log.size());
  };
  CHECK_EQUAL(query(":addr == 169.254.225.22"), res(680u, 682u, 719u, 720u));
  CHECK_EQUAL(rank(query("id.resp_p == 995/?")), 53u);
  CHECK_EQUAL(rank(query("id.resp_p == 5355/?")), 49u);
  CHECK_EQUAL(rank(query("id.resp_p == 995/? || id.resp_p == 5355/?")), 102u);
  CHECK_EQUAL(rank(query("&time > 1970-01-01")), bro_conn_log.size());
  CHECK_EQUAL(rank(query("proto == \"udp\"")), 5306u);
  CHECK_EQUAL(rank(query("proto == \"tcp\"")), 3135u);
  CHECK_EQUAL(rank(query("uid == \"nkCxlvNN8pi\"")), 1u);
  CHECK_EQUAL(rank(query("orig_bytes < 400")), 5332u);
  CHECK_EQUAL(rank(query("orig_bytes < 400 && proto == \"udp\"")), 4357u);
  CHECK_EQUAL(rank(query(":addr == 169.254.225.22")), 4u);
  CHECK_EQUAL(rank(query("service == \"http\"")), 2386u);
  CHECK_EQUAL(rank(query("service == \"http\" && :addr == 212.227.96.110")),
              28u);
}

TEST(multiple partitions bro conn log lookup no messaging) {
  // This test bypasses any messaging by reaching directly into the state of
  // each INDEXER actor.
  using indexer_type = caf::stateful_actor<indexer_state>;
  MESSAGE("ingest bro conn logs into partitions of size " << slice_size);
  std::vector<partition_ptr> partitions;
  auto layout = bro_conn_log_layout();
  for (auto& slice : const_bro_conn_log_slices) {
    auto ptr = make_partition(uuid::random());
    CHECK_EQUAL(exists(ptr->dir()), false);
    CHECK_EQUAL(ptr->dirty(), false);
    auto idx_hdl = ptr->manager().get_or_add(layout).first;
    run();
    auto& idx = deref<indexer_type>(idx_hdl);
    idx.initialize();
    auto& tbl = idx.state.tbl;
    CHECK_EQUAL(tbl.dirty(), false);
    tbl.add(slice);
    CHECK_EQUAL(ptr->dirty(), true);
    CHECK_EQUAL(tbl.dirty(), true);
    partitions.emplace_back(std::move(ptr));
  }
  MESSAGE("make sure all partitions have different IDs, paths, and INDEXERs");
  std::set<uuid> id_set;
  std::set<path> path_set;
  std::set<caf::actor> indexer_set;
  for (size_t i = 0; i < partitions.size(); ++i) {
    auto& part = partitions[i];
    CHECK(id_set.emplace(part->id()).second);
    CHECK(path_set.emplace(part->dir()).second);
    part->manager().for_each([&](const caf::actor& idx_hdl) {
      auto& tbl = deref<indexer_type>(idx_hdl).state.tbl;
      CHECK(indexer_set.emplace(idx_hdl).second);
      CHECK(path_set.emplace(tbl.meta_dir()).second);
      CHECK(path_set.emplace(tbl.data_dir()).second);
      size_t col_id = 0;
      tbl.for_each_column([&](column_index* col) {
        REQUIRE(col != nullptr);
        CHECK(path_set.emplace(col->filename()).second);
        auto idx_offset = std::min((i + 1) * slice_size, bro_conn_log.size());
        CHECK_EQUAL(col->idx().offset(), idx_offset);
        if (col_id == 0) {
          // First (and only) meta field is the type.
          CHECK_EQUAL(col->index_type(), string_type{});
        } else {
          // Data field.
          offset off{col_id - 1};
          auto type_at_offset = layout.at(off);
          REQUIRE_NOT_EQUAL(type_at_offset, nullptr);
          CHECK_EQUAL(col->index_type(), *type_at_offset);
        }
        ++col_id;
      });
    });
  }
  MESSAGE("verify partition content");
  auto res = [&](auto... args) {
    return make_ids({args...}, bro_conn_log.size());
  };
  auto query_all = [&](std::string_view expr_str) {
    ids result;
    auto expr = unbox(to<expression>(expr_str));
    for (auto& part : partitions) {
      part->manager().for_each([&](const caf::actor& idx_hdl) {
        auto& tbl = deref<indexer_type>(idx_hdl).state.tbl;
        result |= unbox(tbl.lookup(expr));
      });
    }
    return result;
  };
  CHECK_EQUAL(query_all(":addr == 169.254.225.22"),
              res(680u, 682u, 719u, 720u));
  CHECK_EQUAL(rank(query_all("id.resp_p == 995/?")), 53u);
  CHECK_EQUAL(rank(query_all("id.resp_p == 5355/?")), 49u);
  CHECK_EQUAL(rank(query_all("id.resp_p == 995/? || id.resp_p == 5355/?")),
              102u);
  CHECK_EQUAL(rank(query_all("&time > 1970-01-01")), bro_conn_log.size());
  CHECK_EQUAL(rank(query_all("proto == \"udp\"")), 5306u);
  CHECK_EQUAL(rank(query_all("proto == \"tcp\"")), 3135u);
  CHECK_EQUAL(rank(query_all("uid == \"nkCxlvNN8pi\"")), 1u);
  CHECK_EQUAL(rank(query_all("orig_bytes < 400")), 5332u);
  CHECK_EQUAL(rank(query_all("orig_bytes < 400 && proto == \"udp\"")), 4357u);
  CHECK_EQUAL(rank(query_all(":addr == 169.254.225.22")), 4u);
  CHECK_EQUAL(rank(query_all("service == \"http\"")), 2386u);
  CHECK_EQUAL(rank(query_all("service == \"http\" && :addr == 212.227.96.110")),
              28u);
}

FIXTURE_SCOPE_END()

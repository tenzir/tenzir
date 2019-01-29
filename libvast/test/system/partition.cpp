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
#include "vast/test/test.hpp"

#include "vast/system/partition.hpp"

#include <caf/atom.hpp>
#include <caf/behavior.hpp>
#include <caf/event_based_actor.hpp>

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/type.hpp"
#include "vast/default_table_slice.hpp"
#include "vast/detail/overload.hpp"
#include "vast/detail/spawn_container_source.hpp"
#include "vast/event.hpp"
#include "vast/ids.hpp"
#include "vast/system/indexer.hpp"
#include "vast/table_slice.hpp"

#include "vast/test/fixtures/dummy_index.hpp"

using namespace vast;
using namespace vast::system;

namespace {

template <class T>
std::vector<std::string> sorted_strings(const std::vector<T>& xs) {
  std::vector<std::string> result;
  for (auto& x : xs)
    result.emplace_back(to_string(x));
  std::sort(result.begin(), result.end());
  return result;
}

template <class T>
struct field {
  using type = T;
  std::string name;
  record_field make() && {
    return {std::move(name), type{}};
  }
};

template <class T>
struct skip_field {
  using type = T;
  std::string name;
  record_field make() && {
    return {std::move(name), type{}.attributes({{"skip"}})};
  }
};

struct fixture : fixtures::dummy_index {
  template <class... Ts>
  static record_type rec(Ts... xs) {
    return {std::move(xs).make()...};
  }

  fixture() {
    wipe_persisted_state();
    min_running_actors = sys.registry().running();
  }

  partition_ptr make_partition() {
    return idx_state->make_partition();
  }

  partition_ptr make_partition(uuid id) {
    return idx_state->make_partition(std::move(id));
  }

  /// @returns how many dummy INDEXER actors are currently running.
  size_t running_indexers() {
    return sys.registry().running() - min_running_actors;
  }

  /// Makes sure no persistet state exists.
  void wipe_persisted_state() {
    rm(state_dir);
  }

  /// A vector with some layouts for testing.
  std::vector<record_type> layouts{
    rec(field<integer_type>{"x"}),
    rec(field<integer_type>{"x"}, field<string_type>{"y"}),
    rec(skip_field<string_type>{"x"}, field<real_type>{"y"}),
  };

  static constexpr size_t total_noskip_fields = 4;

  /// Directory where the manager is supposed to persist its state.
  path state_dir = directory / "indexer-manager";

  /// Number of actors that run before the manager spawns INDEXER actors.
  size_t min_running_actors;

  /// Some UUID for the partition.
  uuid partition_id = uuid::random();
};

} // namespace <anonymous>

FIXTURE_SCOPE(partition_tests, fixture)

TEST(lazy initialization) {
  MESSAGE("create new partition");
  uuid id;
  run_in_index([&] {
    auto put = make_partition();
    id = put->id();
    CHECK_EQUAL(put->dirty(), false);
    MESSAGE("add lazily initialized table indexers");
    for (auto& x : layouts)
      CHECK_EQUAL(put->get_or_add(x).first.init(), caf::none);
    CHECK_EQUAL(put->dirty(), true);
    CHECK_EQUAL(sorted_strings(put->layouts()), sorted_strings(layouts));
    CHECK_EQUAL(running_indexers(), 0u);
  });
  MESSAGE("re-load partition from disk");
  run_in_index([&] {
    auto put = make_partition(id);
    CHECK_EQUAL(put->dirty(), false);
    CHECK_EQUAL(put->init(), caf::none);
    CHECK_EQUAL(sorted_strings(put->layouts()), sorted_strings(layouts));
    CHECK_EQUAL(running_indexers(), 0u);
  });
}

TEST(eager initialization) {
  MESSAGE("create new partition");
  uuid id;
  run_in_index([&] {
    auto put = make_partition();
    id = put->id();
    MESSAGE("add eagerly initialized table indexers");
    for (auto& x : layouts) {
      auto& tbl = put->get_or_add(x).first;
      CHECK_EQUAL(tbl.init(), caf::none);
      tbl.spawn_indexers();
    }
    CHECK_EQUAL(sorted_strings(put->layouts()), sorted_strings(layouts));
    CHECK_EQUAL(running_indexers(), total_noskip_fields);
  });
  MESSAGE("partition destructor must have stopped all INDEXER actors");
  run();
  CHECK_EQUAL(running_indexers(), 0u);
  MESSAGE("re-load partition from disk");
  run_in_index([&] {
    auto put = make_partition(id);
    CHECK_EQUAL(put->init(), caf::none);
    CHECK_EQUAL(sorted_strings(put->layouts()), sorted_strings(layouts));
    CHECK_EQUAL(running_indexers(), 0u);
  });
}

/*
TEST(integer rows lookup) {
  MESSAGE("generate partition for flat integer type");
  put = make_partition();
  MESSAGE("ingest test data (integers)");
  integer_type col_type;
  record_type layout{{"value", col_type}};
  auto rows = make_rows(1, 2, 3, 1, 2, 3, 1, 2, 3);
  auto slice = default_table_slice::make(layout, rows);
  std::vector<table_slice_ptr> slices{slice};
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

TEST(single partition zeek conn log lookup) {
  MESSAGE("generate partiton for zeek conn log");
  put = make_partition();
  MESSAGE("ingest zeek conn logs");
  auto layout = zeek_conn_log_layout();
  auto indexer = put->manager().get_or_add(layout).first;
  detail::spawn_container_source(sys, zeek_conn_log_slices, indexer);
  run();
  MESSAGE("verify partition content");
  auto res = [&](auto... args) {
    return make_ids({args...}, zeek_conn_log.size());
  };
  CHECK_EQUAL(rank(query("id.resp_p == 53/?")), 3u);
  CHECK_EQUAL(rank(query("id.resp_p == 137/?")), 5u);
  CHECK_EQUAL(rank(query("id.resp_p == 53/? || id.resp_p == 137/?")), 8u);
  CHECK_EQUAL(rank(query("&time > 1970-01-01")), zeek_conn_log.size());
  CHECK_EQUAL(rank(query("proto == \"udp\"")), 20u);
  CHECK_EQUAL(rank(query("proto == \"tcp\"")), 0u);
  CHECK_EQUAL(rank(query("uid == \"nkCxlvNN8pi\"")), 1u);
  CHECK_EQUAL(rank(query("orig_bytes < 400")), 17u);
  CHECK_EQUAL(rank(query("orig_bytes < 400 && proto == \"udp\"")), 17u);
  CHECK_EQUAL(rank(query(":addr == fe80::219:e3ff:fee7:5d23")), 1u);
  CHECK_EQUAL(query(":addr == 192.168.1.104"), res(5u, 6u, 9u, 11u));
  CHECK_EQUAL(rank(query("service == \"dns\"")), 11u);
  CHECK_EQUAL(rank(query("service == \"dns\" && :addr == 192.168.1.102")), 4u);
}

TEST(multiple partitions zeek conn log lookup no messaging) {
  // This test bypasses any messaging by reaching directly into the state of
  // each INDEXER actor.
  using indexer_type = caf::stateful_actor<indexer_state>;
  MESSAGE("ingest zeek conn logs into partitions of size " << slice_size);
  std::vector<partition_ptr> partitions;
  auto layout = zeek_conn_log_layout();
  for (auto& slice : zeek_conn_log_slices) {
    auto ptr = make_partition(uuid::random());
    CHECK_EQUAL(exists(ptr->dir()), false);
    CHECK_EQUAL(ptr->dirty(), false);
    auto indexers = ptr->manager().get_or_add(layout).first;
    run();
    for (auto& idx_hdl : indexers) {
      auto& idx = deref<indexer_type>(idx_hdl);
      idx.initialize();
      auto& col = idx.state.col;
      CHECK_EQUAL(col.dirty(), false);
      col.add(slice);
      CHECK_EQUAL(col.dirty(), true);
    }
    CHECK_EQUAL(ptr->dirty(), true);
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
      CHECK(indexer_set.emplace(idx_hdl).second);
    });
  }
  MESSAGE("verify partition content");
  auto res = [&](auto... args) {
    return make_ids({args...}, zeek_conn_log.size());
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
  CHECK_EQUAL(rank(query_all("id.resp_p == 53/?")), 3u);
  CHECK_EQUAL(rank(query_all("id.resp_p == 137/?")), 5u);
  CHECK_EQUAL(rank(query_all("id.resp_p == 53/? || id.resp_p == 137/?")), 8u);
  CHECK_EQUAL(rank(query_all("&time > 1970-01-01")), zeek_conn_log.size());
  CHECK_EQUAL(rank(query_all("proto == \"udp\"")), 20u);
  CHECK_EQUAL(rank(query_all("proto == \"tcp\"")), 0u);
  CHECK_EQUAL(rank(query_all("uid == \"nkCxlvNN8pi\"")), 1u);
  CHECK_EQUAL(rank(query_all("orig_bytes < 400")), 17u);
  CHECK_EQUAL(rank(query_all("orig_bytes < 400 && proto == \"udp\"")), 17u);
  CHECK_EQUAL(rank(query_all(":addr == fe80::219:e3ff:fee7:5d23")), 1u);
  CHECK_EQUAL(query_all(":addr == 192.168.1.104"), res(5u, 6u, 9u, 11u));
  CHECK_EQUAL(rank(query_all("service == \"dns\"")), 11u);
  CHECK_EQUAL(rank(query_all("service == \"dns\" && :addr == 192.168.1.102")),
              4u);
}
*/

FIXTURE_SCOPE_END()

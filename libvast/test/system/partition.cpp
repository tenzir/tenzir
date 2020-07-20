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
#include "vast/test/fixtures/dummy_index.hpp"
#include "vast/test/test.hpp"

#include "vast/caf_table_slice.hpp"
#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/type.hpp"
#include "vast/detail/overload.hpp"
#include "vast/detail/spawn_container_source.hpp"
#include "vast/ids.hpp"
#include "vast/system/evaluator.hpp"
#include "vast/system/indexer.hpp"
#include "vast/system/partition.hpp"
#include "vast/table_slice.hpp"

#include <caf/atom.hpp>
#include <caf/behavior.hpp>
#include <caf/event_based_actor.hpp>

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

  void make_partition() {
    put = idx_state->make_partition();
  }

  partition* get_active_partition(const table_slice_ptr&) {
    return put.get();
  }

  void make_partition(uuid id) {
    put = idx_state->make_partition(std::move(id));
  }

  void reset_partition() {
    put.reset();
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

  void use_real_indexer_actors() {
    // For this test, we use actual INDEXER actors.
    idx_state->factory = system::spawn_indexer;
  }

  // @pre called inside of a `run_in_index` block
  ids query(std::string_view query_str) {
    VAST_ASSERT(put != nullptr);
    auto expr = unbox(to<expression>(query_str));
    auto eval = sys.spawn(system::evaluator, expr, put->eval(expr));
    self->send(eval, self);
    run();
    ids result;
    bool got_done_atom = false;
    while (!self->mailbox().empty())
      self->receive([&](const ids& hits) { result |= hits; },
                    [&](atom::done) { got_done_atom = true; });
    if (!got_done_atom)
      FAIL("evaluator failed to send 'done'");
    return result;
  }

  void ingest(std::vector<table_slice_ptr> slices) {
    VAST_ASSERT(put != nullptr);
    VAST_ASSERT(slices.size() > 0);
    VAST_ASSERT(std::none_of(slices.begin(), slices.end(),
                             [](auto slice) { return slice == nullptr; }));
    for (auto& slice : slices) {
      put->add(slice);
      auto& layout = slice->layout();
      for (size_t column = 0; column < layout.fields.size(); ++column) {
        auto& field = layout.fields[column];
        auto fqf = qualified_record_field{layout.name(), field};
        auto ix = put->indexers_.find(fqf);
        auto sc = table_slice_column{slice, column};
        anon_send(ix->second.indexer, std::vector{sc});
      }
    }
    run();
  }

  void ingest(table_slice_ptr slice) {
    ingest(std::vector{slice});
  }

  /// Directory where the manager is supposed to persist its state.
  path state_dir = directory / "indexer-manager";

  /// Number of actors that run before the manager spawns INDEXER actors.
  size_t min_running_actors;

  /// Some UUID for the partition.
  uuid partition_id = uuid::random();

  /// Partition-under-test.
  partition_ptr put;
};

} // namespace <anonymous>

FIXTURE_SCOPE(partition_tests, fixture)

#if 0
TEST_DISABLED(lazy initialization) {
  MESSAGE("create new partition");
  uuid id;
  run_in_index([&] {
    make_partition();
    id = put->id();
    CHECK_EQUAL(put->dirty(), false);
    MESSAGE("add lazily initialized table indexers");
    for (auto& x : layouts)
      put->add(x);
    CHECK_EQUAL(put->dirty(), true);
    CHECK_EQUAL(sorted_strings(put->layouts()), sorted_strings(layouts));
    CHECK_EQUAL(running_indexers(), 0u);
    reset_partition();
  });
  MESSAGE("re-load partition from disk");
  run_in_index([&] {
    make_partition(id);
    CHECK_EQUAL(put->dirty(), false);
    CHECK_EQUAL(put->init(), caf::none);
    CHECK_EQUAL(sorted_strings(put->layouts()), sorted_strings(layouts));
    CHECK_EQUAL(running_indexers(), 0u);
    reset_partition();
  });
}

TEST_DISABLED(eager initialization) {
  MESSAGE("create new partition");
  uuid id;
  run_in_index([&] {
    make_partition();
    id = put->id();
    MESSAGE("add eagerly initialized table indexers");
    for (auto& x : layouts) {
      auto& tbl = unbox(put->get_or_add(x)).first;
      CHECK_EQUAL(tbl.init(), caf::none);
      tbl.spawn_indexers();
    }
    CHECK_EQUAL(sorted_strings(put->layouts()), sorted_strings(layouts));
    CHECK_EQUAL(running_indexers(), total_noskip_fields);
    reset_partition();
  });
  MESSAGE("partition destructor must have stopped all INDEXER actors");
  run();
  CHECK_EQUAL(running_indexers(), 0u);
  MESSAGE("re-load partition from disk");
  run_in_index([&] {
    make_partition(id);
    CHECK_EQUAL(put->init(), caf::none);
    CHECK_EQUAL(sorted_strings(put->layouts()), sorted_strings(layouts));
    CHECK_EQUAL(running_indexers(), 0u);
    reset_partition();
  });
}
#endif

TEST_DISABLED(zeek conn log http slices) {
  use_real_indexer_actors();
  MESSAGE("scrutinize each zeek conn log slice individually");
  // Pre-computed via:
  //
  //  bro-cut service < test/logs/zeek/conn.log
  //    | awk '{ if ($1 == "http") ++n; if (NR % 100 == 0) { print n; n = 0 } }
  //           END { print n }'
  //    | paste -s -d , -
  //
  // The full list of results is:
  //
  //   13, 16, 20, 22, 31, 11, 14, 28, 13, 42, 45, 52, 59, 54, 59, 59, 51,
  //   29, 21, 31, 20, 28, 9,  56, 48, 57, 32, 53, 25, 31, 25, 44, 38, 55,
  //   40, 23, 31, 27, 23, 59, 23, 2,  62, 29, 1,  5,  7,  0,  10, 5,  52,
  //   39, 2,  0,  9,  8,  0,  13, 4,  2,  13, 2,  36, 33, 17, 48, 50, 27,
  //   44, 9,  94, 63, 74, 66, 5,  54, 21, 7,  2,  3,  21, 7,  2,  14, 7,
  //
  // However, we only check the first 10 values, because the test otherwise
  // takes too long with a runtime of ~12s.
  std::vector<size_t> num_hits{13, 16, 20, 22, 31, 11, 14, 28, 13, 42};
  for (size_t index = 0; index < num_hits.size(); ++index) {
    run_in_index([&] {
      make_partition();
      ingest(zeek_conn_log_full[index]);
      MESSAGE("expecting");
      CHECK_EQUAL(rank(query("service == \"http\"")), num_hits[index]);
      reset_partition();
    });
    run();
  }
}

TEST_DISABLED(integer rows lookup) {
  use_real_indexer_actors();
  run_in_index([&] {
    MESSAGE("generate partition for flat integer type");
    make_partition();
    MESSAGE("ingest test data (integers)");
    integer_type col_type;
    record_type layout{{"value", col_type}};
    auto rows = make_rows(1, 2, 3, 1, 2, 3, 1, 2, 3);
    ingest(caf_table_slice::make(layout, rows));
    MESSAGE("verify partition content");
    auto res = [&](auto... args) { return make_ids({args...}, rows.size()); };
    CHECK_EQUAL(query(":int == +1"), res(0u, 3u, 6u));
    CHECK_EQUAL(query(":int == +2"), res(1u, 4u, 7u));
    CHECK_EQUAL(query("value == +3"), res(2u, 5u, 8u));
    CHECK_EQUAL(query(":int == +4"), ids());
    CHECK_EQUAL(query(":int != +1"), res(1u, 2u, 4u, 5u, 7u, 8u));
    CHECK_EQUAL(query("!(:int == +1)"), res(1u, 2u, 4u, 5u, 7u, 8u));
    CHECK_EQUAL(query(":int > +1 && :int < +3"), res(1u, 4u, 7u));
  });
}

TEST_DISABLED(single partition zeek conn log lookup) {
  use_real_indexer_actors();
  MESSAGE("generate partiton for zeek conn log");
  run_in_index([&] {
    make_partition();
    MESSAGE("ingest zeek conn logs");
    ingest(zeek_conn_log);
    MESSAGE("verify partition content");
    auto res = [&](auto... args) {
      return make_ids({args...}, zeek_conn_log.size());
    };
    CHECK_EQUAL(rank(query("id.resp_p == 53/?")), 3u);
    CHECK_EQUAL(rank(query("id.resp_p == 137/?")), 5u);
    CHECK_EQUAL(rank(query("id.resp_p == 53/? || id.resp_p == 137/?")), 8u);
    CHECK_EQUAL(rank(query("#timestamp > 1970-01-01")), zeek_conn_log.size());
    CHECK_EQUAL(rank(query("proto == \"udp\"")), 20u);
    CHECK_EQUAL(rank(query("proto == \"tcp\"")), 0u);
    CHECK_EQUAL(rank(query("uid == \"nkCxlvNN8pi\"")), 1u);
    CHECK_EQUAL(rank(query("orig_bytes < 400")), 17u);
    CHECK_EQUAL(rank(query("orig_bytes < 400 && proto == \"udp\"")), 17u);
    CHECK_EQUAL(rank(query(":addr == fe80::219:e3ff:fee7:5d23")), 1u);
    CHECK_EQUAL(query(":addr == 192.168.1.104"), res(5u, 6u, 9u, 11u));
    CHECK_EQUAL(rank(query("service == \"dns\"")), 11u);
    CHECK_EQUAL(rank(query("service == \"dns\" && :addr == 192.168.1.102")),
                4u);
  });
}

FIXTURE_SCOPE_END()

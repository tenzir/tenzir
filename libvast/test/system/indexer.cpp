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

#define SUITE indexer

#include "vast/system/indexer.hpp"

#include "vast/test/test.hpp"
#include "vast/test/fixtures/actor_system_and_events.hpp"

#include "vast/bitmap.hpp"
#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/concept/printable/stream.hpp"
#include "vast/concept/printable/vast/error.hpp"
#include "vast/concept/printable/vast/event.hpp"
#include "vast/concept/printable/vast/expression.hpp"
#include "vast/default_table_slice.hpp"
#include "vast/detail/spawn_container_source.hpp"
#include "vast/system/atoms.hpp"
#include "vast/system/evaluator.hpp"
#include "vast/system/spawn_indexer.hpp"
#include "vast/table_slice.hpp"
#include "vast/type.hpp"

using namespace caf;
using namespace vast;

namespace {

struct fixture : fixtures::deterministic_actor_system_and_events {
  void init(const record_type& layout) {
    auto& fields = layout.fields;
    indexers.reserve(fields.size());
    for (size_t column = 0; column < fields.size(); ++column)
      if (!has_skip_attribute(fields[column].type))
        indexers.emplace_back(system::spawn_indexer(self.ptr(), directory,
                                                    fields[column].type,
                                                    column));
    run();
  }

  void ingest(std::vector<table_slice_ptr> slices) {
    VAST_ASSERT(slices.size() > 0);
    auto& layout = slices[0]->layout();
    init(layout);
    VAST_ASSERT(std::all_of(slices.begin(), slices.end(), [&](auto& slice) {
      return slice->layout() == layout;
    }));
    vast::detail::spawn_container_source(sys, std::move(slices), indexers);
    run();
  }

  ids query(std::string_view what) {
    // Spin up an EVALUATOR for collecting the results.
    auto expr = unbox(to<expression>(what));
    auto eval = self->spawn(system::evaluator, indexers);
    self->send(eval, std::move(expr), self);
    run();
    // Fetch results from mailbox.
    ids result;
    bool done = false;
    while (!done)
      self->receive([&](const ids &hits) { result |= hits; },
                    [&](system::done_atom) { done = true; });
    if (result.size() < num_ids)
      result.append_bits(false, num_ids - result.size());
    return result;
  }

  template <class... Ts>
  auto res(Ts... args) {
    return make_ids({args...}, num_ids);
  }

  /// Aligns `x` to the size of `y`.
  void align(ids& x, const ids& y) {
    if (x.size() < y.size())
      x.append_bits(false, y.size() - x.size());
  };

  /// Number size of our ID space.
  size_t num_ids = 0;

  /// Our actors-under-test.
  std::vector<actor> indexers;
};

} // namespace <anonymous>

FIXTURE_SCOPE(indexer_tests, fixture)

TEST(integer rows) {
  MESSAGE("ingest integer events");
  integer_type column_type;
  record_type layout{{"value", column_type}};
  auto rows = make_rows(1, 2, 3, 1, 2, 3, 1, 2, 3);
  num_ids = rows.size();
  ingest({default_table_slice::make(layout, rows)});
  run();
  MESSAGE("verify table index");
  auto verify = [&] {
    CHECK_EQUAL(query(":int == +1"), res(0u, 3u, 6u));
    CHECK_EQUAL(query(":int == +2"), res(1u, 4u, 7u));
    CHECK_EQUAL(query(":int == +3"), res(2u, 5u, 8u));
    CHECK_EQUAL(query(":int == +4"), res());
    CHECK_EQUAL(query(":int != +1"), res(1u, 2u, 4u, 5u, 7u, 8u));
    CHECK_EQUAL(query("!(:int == +1)"), res(1u, 2u, 4u, 5u, 7u, 8u));
    CHECK_EQUAL(query(":int > +1 && :int < +3"), res(1u, 4u, 7u));
  };
  verify();
  MESSAGE("kill INDEXER");
  for (auto& indexer : indexers)
    anon_send_exit(indexer, exit_reason::kill);
  run();
  MESSAGE("reload INDEXER from disk");
  init(layout);
  MESSAGE("verify table index again");
  verify();
}

TEST(bro conn logs) {
  MESSAGE("ingest bro conn log");
  ingest(bro_conn_log_slices);
  MESSAGE("verify table index");
  num_ids = bro_conn_log.size();
  auto verify = [&] {
    CHECK_EQUAL(rank(query("id.resp_p == 53/?")), 3u);
    CHECK_EQUAL(rank(query("id.resp_p == 137/?")), 5u);
    CHECK_EQUAL(rank(query("id.resp_p == 53/? || id.resp_p == 137/?")), 8u);
    CHECK_EQUAL(rank(query("&time > 1970-01-01")), bro_conn_log.size());
    CHECK_EQUAL(rank(query("proto == \"udp\"")), 20u);
    CHECK_EQUAL(rank(query("proto == \"tcp\"")), 0u);
    CHECK_EQUAL(rank(query("uid == \"nkCxlvNN8pi\"")), 1u);
    CHECK_EQUAL(rank(query("orig_bytes < 400")), 17u);
    CHECK_EQUAL(rank(query("orig_bytes < 400 && proto == \"udp\"")), 17u);
    CHECK_EQUAL(rank(query(":addr == fe80::219:e3ff:fee7:5d23")), 1u);
    CHECK_EQUAL(query(":addr == 192.168.1.104"), res(5u, 6u, 9u, 11u));
  };
  verify();
  MESSAGE("kill INDEXER");
  for (auto& indexer: indexers)
    anon_send_exit(indexer, exit_reason::kill);
  indexers.clear();
  run();
  MESSAGE("reload INDEXER from disk");
  init(bro_conn_log_layout());
  MESSAGE("verify table index again");
  verify();
}

FIXTURE_SCOPE_END()

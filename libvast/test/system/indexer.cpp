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
  void init(type col_type) {
    indexer = system::spawn_indexer(self.ptr(), directory, col_type, 0, self,
                                    partition_id);
    run();
  }

  void ingest(std::vector<table_slice_ptr> slices) {
    VAST_ASSERT(slices.size() > 0);
    auto& layout = slices[0]->layout();
    VAST_ASSERT(layout.fields.size() == 1);
    init(layout.fields[0].type);
    VAST_ASSERT(std::all_of(slices.begin(), slices.end(), [&](auto& slice) {
      return slice->layout() == layout;
    }));
    vast::detail::spawn_container_source(sys, std::move(slices), indexer);
    run();
    check_done();
  }

  ids query(std::string_view what) {
    // Spin up an EVALUATOR for collecting the results.
    auto pred = unbox(to<predicate>(what));
    self->send(indexer, curried(pred));
    run();
    // Fetch results from mailbox.
    ids result;
    self->receive([&](const ids& hits) { result |= hits; });
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

  void check_done() {
    bool done = false;
    self->receive([&](system::done_atom, uuid part_id) {
      if (partition_id == part_id)
        done = true;
    });
    CHECK(done);
  }

  /// Number size of our ID space.
  size_t num_ids = 0;

  uuid partition_id = uuid::random();

  /// Our actors-under-test.
  actor indexer;
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
  MESSAGE("verify table index");
  auto verify = [&] {
    CHECK_EQUAL(query(":int == +1"), res(0u, 3u, 6u));
    CHECK_EQUAL(query(":int == +2"), res(1u, 4u, 7u));
    CHECK_EQUAL(query(":int == +3"), res(2u, 5u, 8u));
    CHECK_EQUAL(query(":int == +4"), res());
    CHECK_EQUAL(query(":int != +1"), res(1u, 2u, 4u, 5u, 7u, 8u));
  };
  verify();
  MESSAGE("kill INDEXER");
  anon_send_exit(indexer, exit_reason::kill);
  run();
  MESSAGE("reload INDEXER from disk");
  init(layout.fields[0].type);
  MESSAGE("verify table index again");
  verify();
}

FIXTURE_SCOPE_END()

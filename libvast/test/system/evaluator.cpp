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

#define SUITE evaluator

#include "vast/system/evaluator.hpp"

#include "vast/test/test.hpp"

#include "vast/test/fixtures/actor_system_and_events.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/expression.hpp"
#include "vast/system/atoms.hpp"

using namespace vast;

namespace {

// Dummy actor representing an INDEXER for field `x`.
caf::behavior dummy_indexer(ids result) {
  return {
    [=](curried_predicate) {
      return result;
    }
  };
}

struct fixture : fixtures::deterministic_actor_system_and_events {
  fixture() {
    layout.fields.emplace_back("x", count_type{});
    layout.fields.emplace_back("y", count_type{});
    layout.name("test");
    // Spin up our dummies.
    auto& x_indexers= indexers["x"];
    x_indexers.emplace_back(sys.spawn(dummy_indexer, make_ids({1, 2, 4})));
    x_indexers.emplace_back(sys.spawn(dummy_indexer, make_ids({0, 3, 4})));
    auto& y_indexers= indexers["y"];
    y_indexers.emplace_back(sys.spawn(dummy_indexer, make_ids({4, 8})));
    y_indexers.emplace_back(sys.spawn(dummy_indexer, make_ids({1, 3, 4})));
  }

  /// Maps predicates to a list of actors.
  std::map<std::string, std::vector<caf::actor>> indexers;

  record_type layout;

  ids query(std::string_view expr_str) {
    auto expr = unbox(to<expression>(expr_str));
    evaluation_map qm;
    auto& triples = qm[layout];
    auto resolved = resolve(expr, layout);
    VAST_ASSERT(resolved.size() > 0);
    for (auto& [expr_position, pred]: resolved) {
      VAST_ASSERT(caf::holds_alternative<data_extractor>(pred.lhs));
      auto& dx = caf::get<data_extractor>(pred.lhs);
      std::string field_name = dx.offset.back() == 0 ? "x" : "y";
      auto& xs =  indexers[field_name];
      for (auto& x : xs)
        triples.emplace_back(expr_position,
                             curried_predicate{pred.op, pred.rhs}, x);
    }
    auto eval = sys.spawn(system::evaluator, expr, std::move(qm));
    self->send(eval, self);
    run();
    ids result;
    while (!self->mailbox().empty())
      self->receive([&](const ids& hits) { result |= hits; },
                    [](system::done_atom) {});
    return result;
  }
};

} // namespace

FIXTURE_SCOPE(evaluator_tests, fixture)

TEST(evaluation queries) {
  // `x == 42` returns IDs: [0, 1, 2, 3, 4]
  // `y != 10` returns IDs: [1, 3, 4, 8]
  CHECK_EQUAL(query("x == 42"), make_ids({{0, 5}}));
  CHECK_EQUAL(query("y != 10"), make_ids({1, 3, 4, 8}));
  CHECK_EQUAL(query("x == 42 && y != 10"), make_ids({1, 3, 4}, 9));
  CHECK_EQUAL(query("x == 42 || y != 10"), make_ids({{0, 5}, 8}, 9));
}

FIXTURE_SCOPE_END()

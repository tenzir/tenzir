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

#include "vast/test/fixtures/actor_system_and_events.hpp"
#include "vast/test/test.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/expression.hpp"
#include "vast/fwd.hpp"
#include "vast/system/evaluator.hpp"

#include <vector>

using namespace vast;

namespace {

using counts = std::vector<vast::count>;

template <class F>
ids select(const counts& xs, vast::count y, F pred) {
  ids result;
  for (auto x : xs)
    result.append_bit(pred(x, y));
  return result;
}

ids select(const counts& xs, curried_predicate pred) {
  if (!caf::holds_alternative<vast::count>(pred.rhs))
    FAIL("RHS is not a count");
  auto y = caf::get<vast::count>(pred.rhs);
  switch (pred.op) {
    default:
      FAIL("unsupported relational operator");
    case equal:
      return select(xs, y, std::equal_to<>{});
    case not_equal:
      return select(xs, y, std::not_equal_to<>{});
    case less:
      return select(xs, y, std::less<>{});
    case less_equal:
      return select(xs, y, std::less_equal<>{});
    case greater:
      return select(xs, y, std::greater<>{});
    case greater_equal:
      return select(xs, y, std::greater_equal<>{});
  }
}

// Dummy actor representing an INDEXER for field `x`.
caf::behavior dummy_indexer(counts xs) {
  return {
    [xs = std::move(xs)](curried_predicate pred) { return select(xs, pred); }};
}

struct fixture : fixtures::deterministic_actor_system_and_events {
  fixture() {
    layout.fields.emplace_back("x", count_type{});
    layout.fields.emplace_back("y", count_type{});
    layout.name("test");
    // Spin up our dummies.
    auto& x_indexers= indexers["x"];
    add_indexer(x_indexers, {12, 42, 42, 17, 42, 75, 38, 11, 10});
    add_indexer(x_indexers, {42, 13, 17, 42, 99, 87, 23, 55, 11});
    auto& y_indexers= indexers["y"];
    add_indexer(y_indexers, {10, 10, 10, 10, 42, 10, 10, 10, 42});
    add_indexer(y_indexers, {10, 42, 10, 77, 42, 10, 10, 10, 10});
  }

  /// Maps predicates to a list of actors.
  std::map<std::string, std::vector<caf::actor>> indexers;

  void add_indexer(std::vector<caf::actor>& container, counts data) {
    container.emplace_back(sys.spawn(dummy_indexer, std::move(data)));
  }

  record_type layout;

  ids query(std::string_view expr_str) {
    auto expr = unbox(to<expression>(expr_str));
    evaluation_triples triples;
    auto resolved = resolve(expr, layout);
    VAST_ASSERT(resolved.size() > 0);
    for (auto& [expr_position, pred]: resolved) {
      VAST_ASSERT(caf::holds_alternative<data_extractor>(pred.lhs));
      auto& dx = caf::get<data_extractor>(pred.lhs);
      std::string field_name = dx.offset.back() == 0 ? "x" : "y";
      auto& xs =  indexers[field_name];
      for (auto& x : xs)
        triples.emplace_back(expr_position, curried(pred), x);
    }
    auto eval = sys.spawn(system::evaluator, expr, std::move(triples));
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
};

/// All of our indexers produce results of size 9.
constexpr size_t result_size = 9;

ids pad_result(ids x) {
  if (x.size() < result_size)
    x.append_bits(false, result_size - x.size());
  return x;
}

} // namespace

#define CHECK_QUERY(str, result)                                               \
  CHECK_EQUAL(pad_result(query(str)), pad_result(make_ids result));

FIXTURE_SCOPE(evaluator_tests, fixture)

TEST(simple queries) {
  MESSAGE("no hit in any indexer");
  CHECK_QUERY("x == 98", ({}));
  CHECK_QUERY("y <  10", ({}));
  MESSAGE("hits in one indexer");
  CHECK_QUERY("x == 13", ({1}));
  CHECK_QUERY("y >= 50", ({3}));
  MESSAGE("hits in more than one indexer");
  CHECK_QUERY("x == 42", ({{0, 5}}));
  CHECK_QUERY("y != 10", ({1, 3, 4, 8}));
}

TEST(conjunctions) {
  MESSAGE("no hit on either side");
  CHECK_QUERY("x == 33 && y >= 99", ({}));
  MESSAGE("hits on the left-hand side");
  CHECK_QUERY("x == 13 && y >= 99", ({}));
  MESSAGE("hits on the right-hand side");
  CHECK_QUERY("x == 33 && y != 10", ({}));
  MESSAGE("hits on both sides with intersection");
  CHECK_QUERY("x == 42 && y != 10", ({1, 3, 4}));
  MESSAGE("hits on both sides without intersection");
  CHECK_QUERY("x == 75 && y == 77", ({}));
}

TEST(disjunctions) {
  MESSAGE("no hit on either side");
  CHECK_QUERY("x == 33 || y >= 99", ({}));
  MESSAGE("hits on the left-hand side");
  CHECK_QUERY("x == 13 || y >= 99", ({1}));
  MESSAGE("hits on the right-hand side");
  CHECK_QUERY("x == 33 || y != 10", ({1, 3, 4, 8}));
  MESSAGE("hits on both sides with intersection");
  CHECK_QUERY("x == 42 || y != 10", ({0, 1, 2, 3, 4, 8}));
  MESSAGE("hits on both sides without intersection");
  CHECK_QUERY("x == 75 || y == 77", ({3, 5}));
}

FIXTURE_SCOPE_END()

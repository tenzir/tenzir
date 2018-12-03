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

auto lookup_table(std::initializer_list<std::pair<std::string_view, ids>> xs) {
  std::map<predicate, ids> result;
  for (auto& x : xs)
    result.emplace(unbox(to<predicate>(x.first)), x.second);
  return result;
}

struct fixture : fixtures::deterministic_actor_system_and_events {
  auto new_indexer(std::initializer_list<std::pair<std::string_view, ids>> xs) {
    auto tbl = lookup_table(xs);
    return sys.spawn([=](caf::event_based_actor*) -> caf::behavior {
      return {[=](const predicate& pred) -> caf::result<ids> {
        auto i = tbl.find(pred);
        if (i != tbl.end())
          return i->second;
        return caf::sec::invalid_argument;
      }};
    });
  }

  fixture() {
    indexers = {new_indexer({{"x == 42", make_ids({1, 2, 4})},
                             {"y != 10", make_ids({4, 8})}}),
                new_indexer({{"x == 42", make_ids({0, 3, 4})},
                             {"y != 10", make_ids({1, 3, 4})}})};
  }

  std::vector<caf::actor> indexers;

  ids query(std::string_view expr) {
    auto eval = sys.spawn(system::evaluator, indexers);
    self->send(eval, unbox(to<expression>(expr)), self);
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

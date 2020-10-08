// Copyright Tenzir GmbH. All rights reserved.

#define SUITE taxonomies

#include "vast/taxonomies.hpp"

#include "vast/test/test.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/expression.hpp"

#include <caf/test/dsl.hpp>

using namespace vast;

TEST(concepts) {
  auto c1 = concepts_t{{"foo", {"a.fo0", "b.foO", "x.foe"}},
                       {"bar", {"a.b@r", "b.baR"}}};
  auto models = models_t{};
  auto t1 = taxonomies{std::move(c1), std::move(models)};
  auto exp = unbox(to<expression>("foo == \"1\""));
  auto ref = unbox(to<expression>("a.fo0 == \"1\" || b.foO == \"1\" || x.foe "
                                  "== \"1\""));
  auto result = resolve(t1, exp);
  CHECK_EQUAL(result, ref);
}

// Copyright Tenzir GmbH. All rights reserved.

#define SUITE taxonomies

#include "vast/taxonomies.hpp"

#include "vast/test/test.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/expression.hpp"

#include <caf/test/dsl.hpp>

using namespace vast;

TEST(concepts - simple) {
  auto c = concepts_t{{"foo", {"a.fo0", "b.foO", "x.foe"}},
                      {"bar", {"a.bar", "b.baR"}}};
  auto t = taxonomies{std::move(c), models_t{}};
  auto exp = unbox(to<expression>("foo == \"1\""));
  auto ref = unbox(to<expression>("a.fo0 == \"1\" || b.foO == \"1\" || x.foe "
                                  "== \"1\""));
  auto result = resolve(t, exp);
  CHECK_EQUAL(result, ref);
}

TEST(concepts - cyclic definition) {
  auto c = concepts_t{{"foo", {"bar", "a.fo0", "b.foO", "x.foe"}},
                      {"bar", {"a.bar", "b.baR", "foo"}}};
  auto t = taxonomies{std::move(c), models_t{}};
  auto exp = unbox(to<expression>("foo == \"1\""));
  auto ref
    = unbox(to<expression>("a.fo0 == \"1\" || b.foO == \"1\" || x.foe == \"1\" "
                           "|| a.bar == \"1\" || b.baR == \"1\""));
  auto result = resolve(t, exp);
  CHECK_EQUAL(result, ref);
}

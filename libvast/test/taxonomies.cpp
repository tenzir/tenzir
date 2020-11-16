// Copyright Tenzir GmbH. All rights reserved.

#define SUITE taxonomies

#include "vast/taxonomies.hpp"

#include "vast/test/test.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/expression.hpp"

#include <caf/test/dsl.hpp>

using namespace vast;

TEST(concepts - convert from data) {
  auto x = data{list{
    record{{"concept", record{{"name", "foo"},
                              {"fields", list{"a.fo0", "b.foO", "x.foe"}}}}},
    record{{"concept",
            record{{"name", "bar"}, {"fields", list{"a.bar", "b.baR"}}}}}}};
  auto ref = concepts_map{{{"foo", {"", {"a.fo0", "b.foO", "x.foe"}, {}}},
                           {"bar", {"", {"a.bar", "b.baR"}, {}}}}};
  auto test = unbox(extract_concepts(x));
  CHECK_EQUAL(test, ref);
}

TEST(concepts - simple) {
  auto c = concepts_map{{{"foo", {"", {"a.fo0", "b.foO", "x.foe"}, {}}},
                         {"bar", {"", {"a.bar", "b.baR"}, {}}}}};
  auto t = taxonomies{std::move(c), models_map{}};
  {
    MESSAGE("LHS");
    auto exp = unbox(to<expression>("foo == 1"));
    auto ref = unbox(to<expression>("a.fo0 == 1 || b.foO == 1 || x.foe == 1"));
    auto result = resolve(t, exp);
    CHECK_EQUAL(result, ref);
  }
  {
    MESSAGE("RHS");
    auto exp = unbox(to<expression>("0 in foo"));
    auto ref = unbox(to<expression>("0 in a.fo0 || 0 in b.foO || 0 in x.foe"));
    auto result = resolve(t, exp);
    CHECK_EQUAL(result, ref);
  }
}

TEST(concepts - cyclic definition) {
  // Concepts can reference other concepts in their definition. Two concepts
  // referencing each other create a cycle. This test makes sure that the
  // resolve function does not go into an infinite loop and the result is
  // according to the expectation.
  auto c = concepts_map{{{"foo", {"", {"a.fo0", "b.foO", "x.foe"}, {"bar"}}},
                         {"bar", {"", {"a.bar", "b.baR"}, {"foo"}}}}};
  auto t = taxonomies{std::move(c), models_map{}};
  auto exp = unbox(to<expression>("foo == 1"));
  auto ref = unbox(to<expression>("a.fo0 == 1 || b.foO == 1 || x.foe == 1 || "
                                  "a.bar == 1 || b.baR == 1"));
  auto result = resolve(t, exp);
  CHECK_EQUAL(result, ref);
}

TEST(models - convert from data) {
  auto x = data{list{
    record{{"model", record{{"name", "foo"},
                            {"concepts", list{"a.fo0", "b.foO", "x.foe"}}}}},
    record{{"model", record{{"name", "bar"},
                            {"concepts", list{"a.bar", "b.baR"}},
                            {"models", list{"foo"}}}}}}};
  auto ref = models_map{{{"foo", {"", {"a.fo0", "b.foO", "x.foe"}}},
                         {"bar", {"", {"a.bar", "b.baR", "foo"}}}}};
  auto test = unbox(extract_models(x));
  CHECK_EQUAL(test, ref);
  auto x2 = data{list{
    record{{"model", record{{"name", "foo"},
                            {"concepts", list{"a.fo0", "b.foO", "x.foe"}}}}},
    record{{"model",
            record{{"name", "foo"}, {"concepts", list{"a.bar", "b.baR"}}}}}}};
  auto test2 = extract_models(x2);
  REQUIRE(!test2);
  CHECK_EQUAL(test2.error(), ec::convert_error);
}

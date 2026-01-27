// Copyright Tenzir GmbH. All rights reserved.

#include "tenzir/taxonomies.hpp"

#include "tenzir/concept/parseable/tenzir/expression.hpp"
#include "tenzir/concept/parseable/to.hpp"
#include "tenzir/data.hpp"
#include "tenzir/expression.hpp"
#include "tenzir/test/test.hpp"

using namespace tenzir;

TEST("concepts - convert from data") {
  auto x = data{list{
    record{{"concept", record{{"name", "foo"},
                              {"fields", list{"a.fo0", "b.foO", "x.foe"}}}}},
    record{{"concept",
            record{{"name", "bar"}, {"fields", list{"a.bar", "b.baR"}}}}}}};
  auto ref = concepts_map{{{"foo", {"", {"a.fo0", "b.foO", "x.foe"}, {}}},
                           {"bar", {"", {"a.bar", "b.baR"}, {}}}}};
  concepts_map test;
  CHECK_EQUAL(convert(x, test, concepts_data_schema), caf::error{});
  CHECK_EQUAL(test, ref);
}

TEST("concepts - simple") {
  auto c = concepts_map{{{"foo", {"", {"a.fo0", "b.foO", "x.foe"}, {}}},
                         {"bar", {"", {"a.bar", "b.baR"}, {}}}}};
  {
    MESSAGE("resolve field names");
    auto result = resolve_concepts(c, {"foo", "c.baz"});
    auto expected = std::vector<std::string>{
      "a.fo0",
      "b.foO",
      "x.foe",
      "c.baz",
    };
    CHECK_EQUAL(result, expected);
  }
  auto t = taxonomies{std::move(c)};
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

TEST("concepts - cyclic definition") {
  // Concepts can reference other concepts in their definition. Two concepts
  // referencing each other create a cycle. This test makes sure that the
  // resolve function does not go into an infinite loop and the result is
  // according to the expectation.
  auto c = concepts_map{{{"foo", {"", {"a.fo0", "b.foO", "x.foe"}, {"bar"}}},
                         {"bar", {"", {"a.bar", "b.baR"}, {"foo"}}}}};
  {
    MESSAGE("resolve field names");
    auto result = resolve_concepts(c, {"foo", "c.baz"});
    auto expected = std::vector<std::string>{
      "a.fo0", "b.foO", "x.foe", "a.bar", "b.baR", "c.baz",
    };
    CHECK_EQUAL(result, expected);
  }

  auto t = taxonomies{std::move(c)};
  auto exp = unbox(to<expression>("foo == 1"));
  auto ref = unbox(to<expression>("a.fo0 == 1 || b.foO == 1 || x.foe == 1 || "
                                  "a.bar == 1 || b.baR == 1"));
  auto result = resolve(t, exp);
  CHECK_EQUAL(result, ref);
}

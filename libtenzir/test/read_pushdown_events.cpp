//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/nova/array_builder.hpp"
#include "tenzir/read_pushdown.hpp"
#include "tenzir/session.hpp"
#include "tenzir/test/test.hpp"
#include "tenzir/tql2/parser.hpp"

#include <string>
#include <string_view>
#include <vector>

using namespace tenzir;

namespace {

auto events() -> nova::Events {
  auto builder = nova::ArrayBuilder<nova::Record>{};
  for (auto i = int64_t{0}; i < 6; ++i) {
    builder.record().field("id").data(i);
  }
  auto mask = nova::storage::BitMap::Mutable{6};
  for (auto i : {1, 3, 4}) {
    mask.set(i, true);
  }
  return nova::Events{builder.finish(), std::move(mask).finish(),
                      nova::Events::Meta::make_empty(6, "test.pushdown")};
}

} // namespace

TEST("reader filters narrow existing masks before counting the limit") {
  auto dh = collecting_diagnostic_handler{};
  auto provider = session_provider::make(dh);
  auto session = provider.as_session();
  auto filters = std::vector<nova::Evaluator>{};
  for (auto text : {"id >= 2", "id <= 4"}) {
    auto expr = parse_expression_with_location_override(text, location::unknown,
                                                        session);
    REQUIRE(expr);
    auto evaluator = nova::Evaluator::make(
      std::move(*expr), nova::InstantiateCtx{dh, session.reg()});
    REQUIRE(evaluator);
    filters.push_back(std::move(*evaluator));
  }
  auto remaining = Option<uint64_t>{1};
  auto result = apply_read_pushdown(events(), filters, remaining, dh);
  CHECK_EQUAL(result.length(), 6);
  CHECK_EQUAL(result.active_count(), 1);
  CHECK(result.mask.get(3));
  CHECK_EQUAL(*remaining, 0u);
  CHECK_EQUAL(*result.meta.name.get(3), "test.pushdown");
  CHECK(dh.empty());
}

TEST("reader predicates diagnose active non-boolean rows including nulls") {
  struct TestCase {
    std::string_view predicate;
    uint64_t active_count;
    std::vector<std::string> warnings;
  };
  auto cases = {
    TestCase{"null", 0, {"expected `bool`"}},
    TestCase{"absent", 0, {"event does not have field", "expected `bool`"}},
    TestCase{"null if id == 3 else true", 2, {"expected `bool`"}},
    TestCase{"null if id == 0 else true", 3, {}},
    TestCase{"42", 0, {"expected `bool`"}},
    TestCase{"false", 0, {}},
  };
  for (auto const& test : cases) {
    auto dh = collecting_diagnostic_handler{};
    auto provider = session_provider::make(dh);
    auto session = provider.as_session();
    auto expr = parse_expression_with_location_override(
      test.predicate, location::unknown, session);
    REQUIRE(expr);
    auto evaluator = nova::Evaluator::make(
      std::move(*expr), nova::InstantiateCtx{dh, session.reg()});
    REQUIRE(evaluator);
    auto filters = std::vector<nova::Evaluator>{};
    filters.push_back(std::move(*evaluator));
    auto remaining = Option<uint64_t>{3};
    auto result = apply_read_pushdown(events(), filters, remaining, dh);
    CHECK_EQUAL(result.active_count(), test.active_count);
    CHECK_EQUAL(*remaining, 3 - test.active_count);
    auto diagnostics = std::move(dh).collect();
    REQUIRE_EQUAL(diagnostics.size(), test.warnings.size());
    for (auto i = size_t{0}; i < diagnostics.size(); ++i) {
      CHECK_EQUAL(diagnostics[i].severity, severity::warning);
      CHECK_EQUAL(diagnostics[i].message, test.warnings[i]);
    }
  }
}

TEST("reader predicates do not diagnose fully inactive input") {
  auto dh = collecting_diagnostic_handler{};
  auto provider = session_provider::make(dh);
  auto session = provider.as_session();
  auto expr = parse_expression_with_location_override(
    "absent", location::unknown, session);
  REQUIRE(expr);
  auto evaluator = nova::Evaluator::make(
    std::move(*expr), nova::InstantiateCtx{dh, session.reg()});
  REQUIRE(evaluator);
  auto filters = std::vector<nova::Evaluator>{};
  filters.push_back(std::move(*evaluator));
  auto input = events();
  input.mask = nova::storage::BitMap{input.length(), false};
  auto remaining = Option<uint64_t>{3};
  auto result = apply_read_pushdown(std::move(input), filters, remaining, dh);
  CHECK_EQUAL(result.active_count(), 0);
  CHECK_EQUAL(*remaining, 3u);
  CHECK(dh.empty());
}

TEST("reader limits count active rows across batches") {
  auto dh = collecting_diagnostic_handler{};
  auto remaining = Option<uint64_t>{4};
  auto first = apply_read_pushdown(events(), {}, remaining, dh);
  CHECK_EQUAL(first.active_count(), 3);
  CHECK_EQUAL(*remaining, 1u);
  auto second = apply_read_pushdown(events(), {}, remaining, dh);
  CHECK_EQUAL(second.active_count(), 1);
  CHECK(second.mask.get(1));
  CHECK_EQUAL(*remaining, 0u);
  auto third = apply_read_pushdown(events(), {}, remaining, dh);
  CHECK_EQUAL(third.active_count(), 0);
  CHECK_EQUAL(third.length(), 6);
  CHECK(dh.empty());
}

//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/data.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/location.hpp"
#include "tenzir/nova/array.hpp"
#include "tenzir/nova/array_builder.hpp"
#include "tenzir/nova/const_eval.hpp"
#include "tenzir/nova/eval_ctx.hpp"
#include "tenzir/nova/materialize.hpp"
#include "tenzir/test/test.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/tql2/entity_path.hpp"
#include "tenzir/tql2/registry.hpp"

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

using namespace tenzir;
using namespace tenzir::nova;

namespace {

auto constant(data value) -> ast::expression {
  return ast::expression{
    ast::constant::make(located<data>{std::move(value), location::unknown})};
}

auto root_field(std::string name) -> ast::expression {
  return ast::expression{
    ast::root_field{ast::identifier{std::move(name), location::unknown}}};
}

auto this_expr() -> ast::expression {
  return ast::expression{ast::this_{location::unknown}};
}

auto record_expr(std::vector<std::pair<std::string, ast::expression>> fields)
  -> ast::expression {
  auto items = std::vector<ast::record::item>{};
  for (auto& [name, expr] : fields) {
    items.emplace_back(ast::record::field{
      ast::identifier{std::move(name), location::unknown}, std::move(expr)});
  }
  return ast::expression{
    ast::record{location::unknown, std::move(items), location::unknown}};
}

auto list_expr(std::vector<ast::expression> elements) -> ast::expression {
  auto items = std::vector<ast::list::item>{};
  for (auto& element : elements) {
    items.emplace_back(std::move(element));
  }
  return ast::expression{
    ast::list{location::unknown, std::move(items), location::unknown}};
}

/// A call to the builtin `name`, resolved to the `std` package.
auto call(std::string name, std::vector<ast::expression> args,
          bool method = false) -> ast::expression {
  auto result = ast::function_call{
    ast::entity{{ast::identifier{name, location::unknown}}}, std::move(args),
    location::unknown, method};
  result.fn.ref = entity_path{
    std::string{entity_pkg_std}, {std::move(name)}, entity_ns::fn};
  return ast::expression{std::move(result)};
}

auto first_error(std::vector<diagnostic> const& diags) -> std::string {
  for (auto const& diag : diags) {
    if (diag.severity == severity::error) {
      return diag.message;
    }
  }
  return {};
}

auto error_count(const std::vector<diagnostic>& diags) -> size_t {
  auto count = size_t{0};
  for (const auto& diag : diags) {
    if (diag.severity == severity::error) {
      ++count;
    }
  }
  return count;
}

} // namespace

TEST("const_eval evaluates a scalar constant") {
  auto dh = null_diagnostic_handler{};
  auto reg = registry{};
  auto result = const_eval(constant(std::int64_t{42}), InstantiateCtx{dh, reg});
  REQUIRE(result);
  auto const* value = try_as<Int>(&*result);
  REQUIRE(value);
  CHECK_EQUAL(*value, std::int64_t{42});
}

TEST("const_eval evaluates nested records and lists in order") {
  auto dh = null_diagnostic_handler{};
  auto reg = registry{};
  auto expr = record_expr({
    {"a", constant(std::int64_t{1})},
    {"b", constant(std::string{"x"})},
    {"c", list_expr({constant(std::int64_t{1}), constant(std::string{"a"}),
                     constant(caf::none)})},
  });
  auto result = const_eval(expr, InstantiateCtx{dh, reg});
  REQUIRE(result);
  auto const* record = try_as<Record>(&*result);
  REQUIRE(record);
  auto const* a = try_as<Int>(&record->at("a"));
  auto const* b = try_as<String>(&record->at("b"));
  REQUIRE(a);
  REQUIRE(b);
  CHECK_EQUAL(*a, std::int64_t{1});
  CHECK_EQUAL(*b, "x");
}

TEST("const_eval evaluates unary and binary kernels over the scratch input") {
  auto dh = null_diagnostic_handler{};
  auto reg = registry{};
  auto negated = ast::expression{ast::unary_expr{
    located<ast::unary_op>{ast::unary_op::neg, location::unknown},
    constant(std::int64_t{1})}};
  auto result = const_eval(negated, InstantiateCtx{dh, reg});
  REQUIRE(result);
  CHECK_EQUAL(*try_as<Int>(&*result), std::int64_t{-1});
  auto sum = ast::expression{ast::binary_expr{
    constant(std::int64_t{1}), ast::binary_op::add, constant(std::int64_t{2})}};
  result = const_eval(sum, InstantiateCtx{dh, reg});
  REQUIRE(result);
  CHECK_EQUAL(*try_as<Int>(&*result), std::int64_t{3});
}

TEST("const_eval rejects `this` with a single error") {
  auto dh = collecting_diagnostic_handler{};
  auto reg = registry{};
  auto result = const_eval(this_expr(), InstantiateCtx{dh, reg});
  CHECK(not result);
  auto diags = std::move(dh).collect();
  REQUIRE_EQUAL(diags.size(), size_t{1});
  CHECK(diags[0].severity == severity::error);
  CHECK_EQUAL(diags[0].message, "expected a constant expression");
}

TEST("const_eval rejects field references, also when nested") {
  auto dh = collecting_diagnostic_handler{};
  auto reg = registry{};
  CHECK(not const_eval(root_field("x"), InstantiateCtx{dh, reg}));
  CHECK(not const_eval(record_expr({{"a", root_field("x")}}),
                       InstantiateCtx{dh, reg}));
  auto diags = std::move(dh).collect();
  CHECK_EQUAL(error_count(diags), size_t{2});
  for (const auto& diag : diags) {
    CHECK_EQUAL(diag.message, "expected a constant expression");
  }
}

TEST("try_const_eval forwards no diagnostics when evaluation fails") {
  auto dh = collecting_diagnostic_handler{};
  auto reg = registry{};
  auto result = try_const_eval(this_expr(), InstantiateCtx{dh, reg});
  CHECK(not result);
  CHECK(std::move(dh).collect().empty());
}

TEST("try_const_eval yields the value of a constant expression") {
  auto dh = collecting_diagnostic_handler{};
  auto reg = registry{};
  auto result
    = try_const_eval(constant(std::int64_t{7}), InstantiateCtx{dh, reg});
  REQUIRE(result);
  CHECK_EQUAL(*try_as<Int>(&*result), std::int64_t{7});
  CHECK(std::move(dh).collect().empty());
}

TEST("materialize converts a nested row into legacy data") {
  auto builder = ArrayBuilder<Data>{};
  {
    auto rb = builder.record();
    rb.field("s").data(std::string_view{"str"});
    auto lb = rb.field("l").list();
    lb.data(std::int64_t{1});
    lb.null();
    rb.field("b").data(true);
  }
  auto array = builder.finish();
  REQUIRE_EQUAL(array.length(), 1);
  auto expected = record{
    {"s", std::string{"str"}},
    {"l", list{std::int64_t{1}, caf::none}},
    {"b", true},
  };
  CHECK_EQUAL(materialize_legacy(array.get(0)), data{std::move(expected)});
}

TEST("const_eval rejects secrets") {
  auto dh = collecting_diagnostic_handler{};
  auto const reg = global_registry();
  CHECK(
    not const_eval(call("secret", {constant("k")}), InstantiateCtx{dh, *reg}));
  CHECK_EQUAL(first_error(std::move(dh).collect()),
              "`secret` cannot be used here");
}

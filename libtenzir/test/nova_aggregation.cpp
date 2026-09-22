//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

/// Tests for nova aggregations: `AggregationInstance` as the stateful
/// evaluator of an aggregation call, and `ListFallback` as the way an
/// aggregation serves as a regular function over list rows. `sum` is the
/// implementation under test.

#include "tenzir/diagnostics.hpp"
#include "tenzir/nova/aggregation.hpp"
#include "tenzir/nova/array.hpp"
#include "tenzir/nova/array_builder.hpp"
#include "tenzir/nova/bitmap.hpp"
#include "tenzir/nova/eval.hpp"
#include "tenzir/nova/eval_ctx.hpp"
#include "tenzir/nova/events.hpp"
#include "tenzir/nova/type_system.hpp"
#include "tenzir/option.hpp"
#include "tenzir/test/test.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/tql2/registry.hpp"

#include <chrono>
#include <cstdint>
#include <limits>
#include <string>
#include <variant>
#include <vector>

using namespace tenzir;
using namespace tenzir::nova;

namespace {

/// `Events` over `data` with every row selected and no origin metadata.
auto make_events(Array<Record> data) -> Events {
  auto const length = data.length();
  return Events{std::move(data), storage::BitMap{length, true},
                Events::Meta::make_empty(length)};
}

auto root_field(std::string name) -> ast::expression {
  return ast::expression{
    ast::root_field{ast::identifier{std::move(name), location::unknown}}};
}

/// A call to the builtin `name`, with its entity already resolved to the
/// `std` package so no name resolution pass is needed.
auto call(std::string name, std::vector<ast::expression> args)
  -> ast::expression {
  auto result = ast::function_call{
    ast::entity{{ast::identifier{name, location::unknown}}}, std::move(args),
    location::unknown, false};
  result.fn.ref = entity_path{
    std::string{entity_pkg_std}, {std::move(name)}, entity_ns::fn};
  return ast::expression{std::move(result)};
}

auto bitmap(std::vector<bool> bits) -> storage::BitMap {
  auto builder = storage::BitMap::Builder{};
  for (auto bit : bits) {
    builder.emplace_back(bit);
  }
  return std::move(builder).finish();
}

/// Events with a single field `x` holding the given values.
template <class... Ts>
auto events_of(Ts... values) -> Events {
  auto builder = ArrayBuilder<Record>{};
  (builder.record().field("x").data(values), ...);
  return make_events(builder.finish());
}

auto make_sum(diagnostic_handler& dh) -> Box<AggregationInstance> {
  auto const reg = global_registry();
  auto instance = AggregationInstance::make(call("sum", {root_field("x")}),
                                            InstantiateCtx{dh, *reg});
  REQUIRE(instance);
  return std::move(*instance);
}

template <class T>
auto get_as(Data const& value) -> Option<T> {
  if (auto const* x = std::get_if<T>(&value)) {
    return *x;
  }
  return None{};
}

auto is_null(Data const& value) -> bool {
  return std::holds_alternative<Null>(value);
}

/// Whether row `i` of `data` is an explicit `Null`.
auto is_null_at(Array<Data> const& data, storage::Index i) -> bool {
  return match(
    data,
    [](Array<Null> const&) {
      return true;
    },
    [&](UnionArray const& u) {
      auto null_alt = u.get_alternative<Null>();
      return null_alt.is_some() and null_alt->present.get(i);
    },
    [](auto const&) {
      return false;
    });
}

auto int_at(Array<Data> const& data, storage::Index i) -> Option<std::int64_t> {
  auto ints = data.get_alternative<Int>();
  if (not ints or not ints->present.get(i)) {
    return None{};
  }
  return *ints->data.get(i);
}

auto eval(ast::expression expression, Events events, storage::BitMap mask,
          diagnostic_handler& dh) -> Array<Data> {
  events.mask = std::move(mask);
  auto const reg = global_registry();
  auto evaluator
    = Evaluator::make(std::move(expression), InstantiateCtx{dh, *reg});
  REQUIRE(evaluator);
  return evaluator->eval(events, EvalCtx{dh});
}

} // namespace

TEST("AggregationInstance::make rejects an expression that is not a call") {
  auto dh = collecting_diagnostic_handler{};
  auto const reg = global_registry();
  auto instance
    = AggregationInstance::make(root_field("x"), InstantiateCtx{dh, *reg});
  CHECK(not instance);
  auto diags = std::move(dh).collect();
  REQUIRE_EQUAL(diags.size(), size_t{1});
  CHECK_EQUAL(diags[0].severity, severity::error);
  CHECK_EQUAL(diags[0].message, "expected an aggregation function");
}

TEST("AggregationInstance::make rejects a regular function") {
  auto dh = collecting_diagnostic_handler{};
  auto const reg = global_registry();
  auto instance = AggregationInstance::make(call("length", {root_field("x")}),
                                            InstantiateCtx{dh, *reg});
  CHECK(not instance);
  auto diags = std::move(dh).collect();
  REQUIRE_EQUAL(diags.size(), size_t{1});
  CHECK_EQUAL(diags[0].severity, severity::error);
  CHECK_EQUAL(diags[0].message, "`length` is not an aggregation function");
}

TEST("sum accumulates across batches, honors the mask, and resets") {
  auto dh = collecting_diagnostic_handler{};
  auto sum = make_sum(dh);
  CHECK(is_null(sum->get()));
  sum->update(events_of(Int{1}, Int{2}, Int{3}), EvalCtx{dh});
  CHECK_EQUAL(get_as<Int>(sum->get()), Option{Int{6}});
  auto masked = events_of(Int{10}, Int{20}, Int{30});
  masked.mask = bitmap({true, false, true});
  sum->update(masked, EvalCtx{dh});
  CHECK_EQUAL(get_as<Int>(sum->get()), Option{Int{46}});
  sum->reset();
  CHECK(is_null(sum->get()));
  sum->update(events_of(Int{5}), EvalCtx{dh});
  CHECK_EQUAL(get_as<Int>(sum->get()), Option{Int{5}});
  CHECK(std::move(dh).collect().empty());
}

TEST("sum skips nulls and stays null on an all-null column") {
  auto dh = collecting_diagnostic_handler{};
  auto sum = make_sum(dh);
  auto builder = ArrayBuilder<Record>{};
  builder.record().field("x").null();
  builder.record().field("x").null();
  sum->update(make_events(builder.finish()), EvalCtx{dh});
  CHECK(is_null(sum->get()));
  sum->update(events_of(Int{4}), EvalCtx{dh});
  CHECK_EQUAL(get_as<Int>(sum->get()), Option{Int{4}});
  CHECK(std::move(dh).collect().empty());
}

TEST("sum of int and uint is a uint") {
  auto dh = collecting_diagnostic_handler{};
  auto sum = make_sum(dh);
  sum->update(events_of(Int{1}, UInt{2}), EvalCtx{dh});
  CHECK_EQUAL(get_as<UInt>(sum->get()), Option{UInt{3}});
  CHECK(std::move(dh).collect().empty());
}

TEST("sum of int and float is a float") {
  auto dh = collecting_diagnostic_handler{};
  auto sum = make_sum(dh);
  sum->update(events_of(Int{1}, Float{2.5}), EvalCtx{dh});
  CHECK_EQUAL(get_as<Float>(sum->get()), Option{Float{3.5}});
  CHECK(std::move(dh).collect().empty());
}

TEST("sum of durations is a duration") {
  auto dh = collecting_diagnostic_handler{};
  auto sum = make_sum(dh);
  sum->update(events_of(Duration{std::chrono::seconds{1}},
                        Duration{std::chrono::seconds{2}}),
              EvalCtx{dh});
  CHECK_EQUAL(get_as<Duration>(sum->get()),
              Option{Duration{std::chrono::seconds{3}}});
  CHECK(std::move(dh).collect().empty());
}

TEST("sum of a number and a duration warns and becomes null") {
  auto dh = collecting_diagnostic_handler{};
  auto sum = make_sum(dh);
  sum->update(events_of(Int{1}), EvalCtx{dh});
  sum->update(events_of(Duration{std::chrono::seconds{1}}), EvalCtx{dh});
  CHECK(is_null(sum->get()));
  // Poisoned sums stay null, without repeating the warning.
  sum->update(events_of(Int{1}), EvalCtx{dh});
  CHECK(is_null(sum->get()));
  auto diags = std::move(dh).collect();
  REQUIRE_EQUAL(diags.size(), size_t{1});
  CHECK_EQUAL(diags[0].severity, severity::warning);
  CHECK_EQUAL(diags[0].message, "got incompatible types `int` and `duration`");
}

TEST("sum warns on integer overflow and becomes null") {
  auto dh = collecting_diagnostic_handler{};
  auto sum = make_sum(dh);
  sum->update(events_of(std::numeric_limits<Int>::max(), Int{1}), EvalCtx{dh});
  CHECK(is_null(sum->get()));
  auto diags = std::move(dh).collect();
  REQUIRE_EQUAL(diags.size(), size_t{1});
  CHECK_EQUAL(diags[0].message, "integer overflow");
}

TEST("sum warns on a non-numeric type and becomes null") {
  auto dh = collecting_diagnostic_handler{};
  auto sum = make_sum(dh);
  sum->update(events_of(Int{1}, std::string_view{"two"}), EvalCtx{dh});
  CHECK(is_null(sum->get()));
  auto diags = std::move(dh).collect();
  REQUIRE_EQUAL(diags.size(), size_t{1});
  CHECK_EQUAL(diags[0].message,
              "expected `int`, `uint`, `float` or `duration`, got `string`");
}

TEST("sum as a function aggregates each list row") {
  auto builder = ArrayBuilder<Record>{};
  {
    auto list = builder.record().field("xs").list();
    list.data(Int{1});
    list.data(Int{2});
    list.data(Int{3});
  }
  builder.record().field("xs").null();
  builder.record().field("xs").data(std::string_view{"not a list"});
  builder.record().field("xs").list();
  {
    auto list = builder.record().field("xs").list();
    list.data(Int{10});
  }
  builder.record().field("xs").data(std::string_view{"masked out"});
  auto events = make_events(builder.finish());
  auto dh = collecting_diagnostic_handler{};
  auto result = eval(call("sum", {root_field("xs")}), events,
                     bitmap({true, true, true, true, true, false}), dh);
  CHECK_EQUAL(result.length(), 6);
  CHECK_EQUAL(int_at(result, 0), Option{Int{6}});
  CHECK(is_null_at(result, 1));
  CHECK(is_null_at(result, 2));
  CHECK(is_null_at(result, 3));
  CHECK_EQUAL(int_at(result, 4), Option{Int{10}});
  // One warning for the non-list row inside the mask, none for the one
  // outside it and none for the null.
  auto diags = std::move(dh).collect();
  REQUIRE_EQUAL(diags.size(), size_t{1});
  CHECK_EQUAL(diags[0].severity, severity::warning);
  CHECK_EQUAL(diags[0].message, "expected `list`, got a different type");
}

TEST("sum as a function handles a constant list column") {
  auto builder = ArrayBuilder<Record>{};
  for (auto i = 0; i < 3; ++i) {
    builder.record().field("y").data(Int{i});
  }
  auto input = builder.finish().with_field_overwrite(
    "xs", {repeat(Data{List{Int{1}, Int{2}}}, 3), storage::BitMap{3, true}});
  auto dh = collecting_diagnostic_handler{};
  auto result = eval(call("sum", {root_field("xs")}), make_events(input),
                     bitmap({true, false, true}), dh);
  CHECK_EQUAL(result.length(), 3);
  CHECK_EQUAL(int_at(result, 0), Option{Int{3}});
  CHECK_EQUAL(int_at(result, 2), Option{Int{3}});
  CHECK(std::move(dh).collect().empty());
}

TEST("sum as a function on a non-list column warns and is null") {
  auto events = events_of(Int{1}, Int{2});
  auto dh = collecting_diagnostic_handler{};
  auto result
    = eval(call("sum", {root_field("x")}), events, bitmap({true, true}), dh);
  CHECK(is_null_at(result, 0));
  CHECK(is_null_at(result, 1));
  auto diags = std::move(dh).collect();
  REQUIRE_EQUAL(diags.size(), size_t{1});
  CHECK_EQUAL(diags[0].message, "expected `list`, got a different type");
}

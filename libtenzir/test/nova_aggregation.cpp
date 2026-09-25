//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

/// Tests for nova aggregations: `AggregationInstance` as the stateful
/// evaluator of an aggregation call, `Aggregation` with one state per group,
/// and the static function kernel through which an aggregation serves as a
/// regular function over list rows. `sum` is the main implementation under
/// test; `count_if` covers aggregations that evaluate a lambda per batch.

#include "tenzir/diagnostics.hpp"
#include "tenzir/hash/hash.hpp"
#include "tenzir/hash/xxhash.hpp"
#include "tenzir/nova/aggregation.hpp"
#include "tenzir/nova/array.hpp"
#include "tenzir/nova/array_builder.hpp"
#include "tenzir/nova/bitmap.hpp"
#include "tenzir/nova/eval.hpp"
#include "tenzir/nova/eval_ctx.hpp"
#include "tenzir/nova/events.hpp"
#include "tenzir/nova/materialize.hpp"
#include "tenzir/nova/type_system.hpp"
#include "tenzir/option.hpp"
#include "tenzir/test/nova.hpp"
#include "tenzir/test/test.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/tql2/registry.hpp"
#include "tenzir/view3.hpp"

#include <algorithm>
#include <bit>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <string>
#include <string_view>
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

auto constant(data value) -> ast::expression {
  return ast::expression{
    ast::constant::make(located<data>{std::move(value), location::unknown})};
}

auto lambda(std::string parameter, ast::expression body) -> ast::expression {
  return ast::expression{
    ast::lambda_expr{ast::identifier{std::move(parameter), location::unknown},
                     location::unknown, std::move(body)}};
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
  auto append = [&](auto value) {
    if constexpr (std::same_as<decltype(value), Null>) {
      builder.record().field("x").null();
    } else {
      builder.record().field("x").data(value);
    }
  };
  (append(values), ...);
  return make_events(builder.finish());
}

auto make_sum(diagnostic_handler& dh) -> Box<AggregationInstance> {
  auto const reg = global_registry();
  auto instance
    = tenzir::test::make_aggregation(call("sum", {root_field("x")}), dh, *reg);
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
    = tenzir::test::make_evaluator(std::move(expression), dh, *reg);
  REQUIRE(evaluator);
  return evaluator->eval(events, EvalCtx{dh});
}

} // namespace

TEST("AggregationInstance::make rejects an expression that is not a call") {
  auto dh = collecting_diagnostic_handler{};
  auto const reg = global_registry();
  auto instance = tenzir::test::make_aggregation(root_field("x"), dh, *reg);
  CHECK(not instance);
  auto diags = std::move(dh).collect();
  REQUIRE_EQUAL(diags.size(), size_t{1});
  CHECK_EQUAL(diags[0].severity, severity::error);
  CHECK_EQUAL(diags[0].message, "expected an aggregation function");
}

TEST("AggregationInstance::make rejects a regular function") {
  auto dh = collecting_diagnostic_handler{};
  auto const reg = global_registry();
  auto instance = tenzir::test::make_aggregation(
    call("length", {root_field("x")}), dh, *reg);
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

TEST("Aggregation folds each group's rows into its own state") {
  auto dh = collecting_diagnostic_handler{};
  auto const reg = global_registry();
  auto aggregation = Aggregation::make(call("sum", {root_field("x")}),
                                       InstantiateCtx{dh, *reg});
  REQUIRE(aggregation);
  auto a = (*aggregation)->make_state();
  auto b = (*aggregation)->make_state();
  auto empty = (*aggregation)->make_state();
  auto events = events_of(Int{1}, Int{10}, Int{2}, Int{20}, Int{100});
  // The last row is inactive, so it must reach no group.
  events.mask = bitmap({true, true, true, true, false});
  auto const a_rows = std::vector<storage::Index>{0, 2};
  auto const b_rows = std::vector<storage::Index>{1, 3};
  auto const groups = std::vector<AggregationGroup>{
    {*a, a_rows},
    {*b, b_rows},
    {*empty, {}},
  };
  (*aggregation)->update(events, groups, EvalCtx{dh});
  CHECK_EQUAL(get_as<Int>(a->get()), Option{Int{3}});
  CHECK_EQUAL(get_as<Int>(b->get()), Option{Int{30}});
  CHECK(is_null(empty->get()));
  // States accumulate across batches and reset independently.
  (*aggregation)->update(events_of(Int{5}), *a, EvalCtx{dh});
  CHECK_EQUAL(get_as<Int>(a->get()), Option{Int{8}});
  a->reset();
  CHECK(is_null(a->get()));
  CHECK_EQUAL(get_as<Int>(b->get()), Option{Int{30}});
  CHECK(std::move(dh).collect().empty());
}

TEST("count_if evaluates its predicate once per batch across groups") {
  auto builder = ArrayBuilder<Record>{};
  auto const add_row = [&](Option<Int> x, Int t) {
    auto row = builder.record();
    if (x) {
      row.field("x").data(*x);
    } else {
      row.field("x").null();
    }
    row.field("t").data(t);
  };
  add_row(Int{1}, Int{0});
  add_row(Int{5}, Int{2});
  add_row(None{}, Int{2});
  add_row(Int{3}, Int{4});
  add_row(Int{7}, Int{0});
  auto events = make_events(builder.finish());
  // The last row is inactive, so it must reach no group.
  events.mask = bitmap({true, true, true, true, false});
  auto const a_rows = std::vector<storage::Index>{0, 1, 2};
  auto const b_rows = std::vector<storage::Index>{3};
  auto const reg = global_registry();
  auto const count_if = [&](ast::expression body, diagnostic_handler& dh) {
    auto aggregation = Aggregation::make(
      call("count_if", {root_field("x"), lambda("v", std::move(body))}),
      InstantiateCtx{dh, *reg});
    REQUIRE(aggregation);
    return std::move(*aggregation);
  };
  {
    // The predicate captures `t` from the row of each value, and nulls never
    // reach it.
    auto dh = collecting_diagnostic_handler{};
    auto aggregation
      = count_if(ast::expression{ast::binary_expr{
                   root_field("v"), ast::binary_op::gt, root_field("t")}},
                 dh);
    auto a = aggregation->make_state();
    auto b = aggregation->make_state();
    auto const groups
      = std::vector<AggregationGroup>{{*a, a_rows}, {*b, b_rows}};
    aggregation->update(events, groups, EvalCtx{dh});
    CHECK_EQUAL(get_as<Int>(a->get()), Option{Int{2}});
    CHECK_EQUAL(get_as<Int>(b->get()), Option{Int{0}});
    CHECK(std::move(dh).collect().empty());
  }
  {
    // A predicate that evaluated once per group would warn once per group.
    auto dh = collecting_diagnostic_handler{};
    auto aggregation = count_if(constant(data{int64_t{1}}), dh);
    auto a = aggregation->make_state();
    auto b = aggregation->make_state();
    auto const groups
      = std::vector<AggregationGroup>{{*a, a_rows}, {*b, b_rows}};
    aggregation->update(events, groups, EvalCtx{dh});
    CHECK_EQUAL(get_as<Int>(a->get()), Option{Int{0}});
    CHECK_EQUAL(get_as<Int>(b->get()), Option{Int{0}});
    CHECK_EQUAL(std::move(dh).collect().size(), 1u);
  }
}

TEST("sketch aggregations honor masks, accumulate across batches, and reset") {
  for (auto name : {"hll", "tdigest", "quantile", "median"}) {
    auto dh = collecting_diagnostic_handler{};
    auto reg = global_registry();
    auto aggregate
      = tenzir::test::make_aggregation(call(name, {root_field("x")}), dh, *reg);
    REQUIRE(aggregate);
    CHECK(is_null((*aggregate)->get()));
    auto events = events_of(Int{1}, std::string_view{"masked out"}, Int{3});
    events.mask = bitmap({true, false, true});
    (*aggregate)->update(events, EvalCtx{dh});
    // Reading the sketch may flush buffered input, but must allow more updates.
    std::ignore = (*aggregate)->get();
    (*aggregate)->update(events_of(Float{5.0}, Null{}), EvalCtx{dh});
    auto result = (*aggregate)->get();
    if (auto record = try_as<Record>(result)) {
      CHECK_EQUAL(get_as<UInt>(record->at("count")), Option{UInt{3}});
      CHECK_EQUAL(get_as<UInt>(record->at("null_count")), Option{UInt{1}});
    } else {
      CHECK_EQUAL(get_as<Float>(result), Option{Float{3.0}});
    }
    (*aggregate)->reset();
    CHECK(is_null((*aggregate)->get()));
    (*aggregate)->update(events_of(Int{9}), EvalCtx{dh});
    result = (*aggregate)->get();
    if (auto record = try_as<Record>(result)) {
      CHECK_EQUAL(get_as<UInt>(record->at("count")), Option{UInt{1}});
      CHECK_EQUAL(get_as<UInt>(record->at("null_count")), Option{UInt{0}});
    } else {
      CHECK_EQUAL(get_as<Float>(result), Option{Float{9.0}});
    }
    CHECK(std::move(dh).collect().empty());
  }
}

TEST("sketch list kernels handle constants and masked invalid rows") {
  for (auto name : {"hll", "tdigest", "quantile", "median"}) {
    auto dh = collecting_diagnostic_handler{};
    auto input = events_of(Int{0}, Int{1}, Int{2});
    input.data = input.data.with_field_overwrite(
      "xs", {repeat(Data{List{Int{1}, Int{2}, Int{3}, Null{}}}, 3),
             storage::BitMap{3, true}});
    auto result = eval(call(name, {root_field("xs")}), input,
                       bitmap({true, false, true}), dh);
    for (auto row : {0, 2}) {
      auto value = to_data(result.get(row));
      if (auto record = try_as<Record>(value)) {
        CHECK_EQUAL(get_as<UInt>(record->at("count")), Option{UInt{3}});
        CHECK_EQUAL(get_as<UInt>(record->at("null_count")), Option{UInt{1}});
      } else {
        CHECK_EQUAL(get_as<Float>(value), Option{Float{2.0}});
      }
    }
    auto builder = ArrayBuilder<Record>{};
    builder.record().field("xs").list().data(Int{7});
    builder.record().field("xs").data(std::string_view{"masked out"});
    builder.record().field("xs").null();
    result = eval(call(name, {root_field("xs")}), make_events(builder.finish()),
                  bitmap({true, false, true}), dh);
    CHECK(is_null_at(result, 2));
    CHECK(std::move(dh).collect().empty());
  }
}

TEST("HLL registers retain the persisted type-sensitive hash contract") {
  auto values = List{
    Int{42},
    UInt{42},
    Float{42},
    std::string{"42"},
    true,
    Duration{42},
    Time{Duration{42}},
    Blob{std::byte{42}},
    List{Int{1}, Null{}, std::string{"x"}},
    Record{{"a", Int{1}}, {"b", List{true, Null{}}}},
  };
  auto expected = std::vector<uint8_t>(size_t{1} << 14, 0);
  auto builder = ArrayBuilder<Record>{};
  for (auto const& value : values) {
    auto field = builder.record().field("x");
    append_data(field, value);
    auto owned = materialize_legacy(value);
    // Hash through the persisted data contract, not through another executor.
    auto digest = tenzir::hash<xxh3_64>(make_view(owned));
    auto index = digest >> (64 - 14);
    auto rank
      = static_cast<uint8_t>(std::min(std::countl_zero(digest << 14) + 1, 51));
    expected[index] = std::max(expected[index], rank);
  }
  auto dh = collecting_diagnostic_handler{};
  auto reg = global_registry();
  auto aggregate
    = tenzir::test::make_aggregation(call("hll", {root_field("x")}), dh, *reg);
  REQUIRE(aggregate);
  (*aggregate)->update(make_events(builder.finish()), EvalCtx{dh});
  auto value = (*aggregate)->get();
  auto const& registers = as<List>(as<Record>(value).at("registers"));
  REQUIRE_EQUAL(registers.size(), expected.size());
  for (auto i = size_t{0}; i < expected.size(); ++i) {
    CHECK_EQUAL(get_as<UInt>(registers[i]), Option{UInt{expected[i]}});
  }
  CHECK(std::move(dh).collect().empty());
}

TEST("quantile narrows duration limits without overflowing") {
  auto dh = collecting_diagnostic_handler{};
  auto reg = global_registry();
  auto aggregate = tenzir::test::make_aggregation(
    call("quantile", {root_field("x")}), dh, *reg);
  REQUIRE(aggregate);
  for (auto count : {std::numeric_limits<Duration::rep>::min(),
                     std::numeric_limits<Duration::rep>::max()}) {
    (*aggregate)->reset();
    (*aggregate)->update(events_of(Duration{count}), EvalCtx{dh});
    CHECK_EQUAL(get_as<Duration>((*aggregate)->get()), Option{Duration{count}});
  }
  CHECK(std::move(dh).collect().empty());
}

TEST("quantile and median list warnings are scoped to an evaluation") {
  auto builder = ArrayBuilder<Record>{};
  for (auto const& values : {
         List{true},
         List{std::string{"bad"}},
         List{std::string{"also bad"}},
         List{Int{1}, Duration{1}},
         List{Int{2}, Duration{2}},
         List{Int{7}},
         List{Duration{9}},
         List{},
       }) {
    auto field = builder.record().field("xs");
    append_data(field, Data{values});
  }
  builder.record().field("xs").null();
  auto events = make_events(builder.finish());
  events.mask = bitmap({false, true, true, true, true, true, true, true, true});
  for (auto name : {"quantile", "median"}) {
    auto prepare_dh = collecting_diagnostic_handler{};
    auto reg = global_registry();
    auto evaluator = tenzir::test::make_evaluator(
      call(name, {root_field("xs")}), prepare_dh, *reg);
    REQUIRE(evaluator);
    CHECK(std::move(prepare_dh).collect().empty());
    // Deduplication spans list rows, but not subsequent evaluations.
    for (auto batch = 0; batch < 2; ++batch) {
      auto dh = collecting_diagnostic_handler{};
      auto result = evaluator->eval(events, EvalCtx{dh});
      for (auto row : {1, 2, 3, 4, 7, 8}) {
        CHECK(is_null_at(result, row));
      }
      // Failed rows must not poison later rows or fix their numeric kind.
      CHECK_EQUAL(get_as<Float>(to_data(result.get(5))), Option{Float{7.0}});
      CHECK_EQUAL(get_as<Duration>(to_data(result.get(6))),
                  Option{Duration{9}});
      auto diagnostics = std::move(dh).collect();
      REQUIRE_EQUAL(diagnostics.size(), size_t{2});
      CHECK_EQUAL(diagnostics[0].message, "expected `int`, `uint`, `float` or "
                                          "`duration`, got `string`");
      CHECK_EQUAL(diagnostics[1].message,
                  "got incompatible types `number` and `duration`");
    }
  }
}

TEST("t-digest aggregation type warnings survive resets") {
  auto dh = collecting_diagnostic_handler{};
  auto reg = global_registry();
  auto aggregate = tenzir::test::make_aggregation(
    call("tdigest", {root_field("x")}), dh, *reg);
  REQUIRE(aggregate);
  for (auto value : {Int{1}, Int{2}, Int{3}}) {
    (*aggregate)->update(events_of(std::string_view{"bad"}, value), EvalCtx{dh});
    (*aggregate)
      ->update(events_of(std::string_view{"bad"}, Null{}), EvalCtx{dh});
    auto result = (*aggregate)->get();
    auto const& model = as<Record>(result);
    CHECK_EQUAL(get_as<UInt>(model.at("input_count")), Option{UInt{4}});
    CHECK_EQUAL(get_as<UInt>(model.at("count")), Option{UInt{1}});
    CHECK_EQUAL(get_as<UInt>(model.at("null_count")), Option{UInt{1}});
    CHECK_EQUAL(get_as<Float>(model.at("min")),
                Option{static_cast<Float>(value)});
    CHECK_EQUAL(get_as<Float>(model.at("max")),
                Option{static_cast<Float>(value)});
    (*aggregate)->reset();
    CHECK(is_null((*aggregate)->get()));
  }
  auto diagnostics = std::move(dh).collect();
  REQUIRE_EQUAL(diagnostics.size(), size_t{1});
  CHECK_EQUAL(diagnostics[0].message,
              "expected `int`, `uint`, or `float`, got `string`; "
              "skipping these values");
}

TEST("t-digest list type warnings are scoped to an evaluation") {
  auto builder = ArrayBuilder<Record>{};
  for (auto value : {Int{7}, Int{9}}) {
    auto list = builder.record().field("xs").list();
    list.data(std::string_view{"bad"});
    list.data(value);
  }
  auto events = make_events(builder.finish());
  auto prepare_dh = collecting_diagnostic_handler{};
  auto reg = global_registry();
  auto evaluator = tenzir::test::make_evaluator(
    call("tdigest", {root_field("xs")}), prepare_dh, *reg);
  REQUIRE(evaluator);
  CHECK(std::move(prepare_dh).collect().empty());
  // Reusing a call site must not suppress warnings in subsequent evaluations.
  for (auto batch = 0; batch < 2; ++batch) {
    auto dh = collecting_diagnostic_handler{};
    auto result = evaluator->eval(events, EvalCtx{dh});
    for (auto row = 0; row < 2; ++row) {
      auto value = to_data(result.get(row));
      auto const& model = as<Record>(value);
      CHECK_EQUAL(get_as<UInt>(model.at("input_count")), Option{UInt{2}});
      CHECK_EQUAL(get_as<UInt>(model.at("count")), Option{UInt{1}});
      CHECK_EQUAL(get_as<Float>(model.at("min")), Option{Float{7.0 + row * 2}});
      CHECK_EQUAL(get_as<Float>(model.at("max")), Option{Float{7.0 + row * 2}});
    }
    auto diagnostics = std::move(dh).collect();
    REQUIRE_EQUAL(diagnostics.size(), size_t{1});
    CHECK_EQUAL(diagnostics[0].message,
                "expected `int`, `uint`, or `float`, got `string`; "
                "skipping these values");
  }
}

TEST("t-digest accessors propagate null and ignore inactive invalid queries") {
  auto dh = collecting_diagnostic_handler{};
  auto reg = global_registry();
  auto aggregate = tenzir::test::make_aggregation(
    call("tdigest", {root_field("x")}), dh, *reg);
  REQUIRE(aggregate);
  (*aggregate)->update(events_of(Int{1}, Int{2}, Int{3}), EvalCtx{dh});
  auto input = events_of(Float{0.5}, std::string_view{"masked out"}, Null{});
  input.data = input.data.with_field_overwrite(
    "model", {repeat((*aggregate)->get(), 3), storage::BitMap{3, true}});
  for (auto name : {"tdigest_quantile", "tdigest_cdf"}) {
    auto result = eval(call(name, {root_field("model"), root_field("x")}),
                       input, bitmap({true, false, true}), dh);
    auto floats = result.get_alternative<Float>();
    REQUIRE(floats);
    REQUIRE(floats->present.get(0));
    CHECK_EQUAL(*floats->data.get(0),
                std::string_view{name} == "tdigest_quantile" ? 2.0 : 0.0);
    CHECK(is_null_at(result, 2));
  }
  CHECK(std::move(dh).collect().empty());
}

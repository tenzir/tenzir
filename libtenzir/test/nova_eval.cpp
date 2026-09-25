//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/base_ctx.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/location.hpp"
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

#include <array>
#include <chrono>
#include <cstdint>
#include <limits>
#include <vector>

using namespace tenzir::nova;
using tenzir::None;
using tenzir::Option;

namespace {

auto eval(tenzir::ast::expression expression, Events events,
          storage::BitMap mask, tenzir::diagnostic_handler& dh,
          tenzir::registry const& reg) -> Array<Data> {
  events.mask = std::move(mask);
  auto evaluator = tenzir::test::make_evaluator(std::move(expression), dh, reg);
  REQUIRE(evaluator);
  return evaluator->eval(events, EvalCtx{dh});
}

/// `Events` over `data` with every row selected and no origin metadata.
auto make_events(Array<Record> data) -> Events {
  auto const length = data.length();
  return Events{std::move(data), storage::BitMap{length, true},
                Events::Meta::make_empty(length)};
}

auto all_rows(Events const& events) -> storage::BitMap {
  return storage::BitMap{events.length(), true};
}

/// A single-column, single-field `Events` value: a record array with one
/// field `x` holding `Int` values.
auto make_int_field_events(std::vector<std::int64_t> values) -> Events {
  auto builder = ArrayBuilder<Record>{};
  for (auto v : values) {
    builder.record().field("x").data(v);
  }
  return make_events(builder.finish());
}

auto root_field(std::string name) -> tenzir::ast::expression {
  return tenzir::ast::expression{tenzir::ast::root_field{
    tenzir::ast::identifier{std::move(name), tenzir::location::unknown}}};
}

auto null_const() -> tenzir::ast::expression {
  return tenzir::ast::expression{tenzir::ast::constant::make(
    tenzir::located<tenzir::data>{tenzir::data{caf::none},
                                  tenzir::location::unknown})};
}

auto empty_list() -> tenzir::ast::expression {
  return tenzir::ast::expression{tenzir::ast::list{
    tenzir::location::unknown, {}, tenzir::location::unknown}};
}

auto empty_record() -> tenzir::ast::expression {
  return tenzir::ast::expression{tenzir::ast::record{
    tenzir::location::unknown, {}, tenzir::location::unknown}};
}

/// Concrete alternatives of `Array<Data>` are reconstructed by value.
/// Use logical matching or the member `try_as<Tag>()` to extract them.
auto as_int_array(const Array<Data>& data) -> Option<Array<Int>> {
  return match(
    data,
    [](Array<Int> arr) -> Option<Array<Int>> {
      return arr;
    },
    [](const auto&) -> Option<Array<Int>> {
      return None{};
    });
}

/// `UnionArray` is stored directly (not reconstructed by value), so unlike
/// `as_int_array` this could also use `try_as<UnionArray>` — `match` is used
/// here regardless, to stay consistent with `as_int_array`'s style.
auto as_union_array(const Array<Data>& data) -> Option<UnionArray> {
  return match(
    data,
    [](const UnionArray& u) -> Option<UnionArray> {
      return u;
    },
    [](const auto&) -> Option<UnionArray> {
      return None{};
    });
}

auto as_bool_array(const Array<Data>& data) -> Option<Array<Bool>> {
  return match(
    data,
    [](Array<Bool> arr) -> Option<Array<Bool>> {
      return arr;
    },
    [](const auto&) -> Option<Array<Bool>> {
      return None{};
    });
}

auto is_null_array(const Array<Data>& data) -> bool {
  return match(
    data,
    [](Array<Null>) {
      return true;
    },
    [](const auto&) {
      return false;
    });
}

} // namespace

TEST("unary not propagates null without a diagnostic") {
  auto events = make_int_field_events({1, 2});
  auto dh = tenzir::collecting_diagnostic_handler{};
  auto reg = tenzir::registry{};
  auto expr = tenzir::ast::expression{tenzir::ast::unary_expr{
    tenzir::located<tenzir::ast::unary_op>{tenzir::ast::unary_op::not_,
                                           tenzir::location::unknown},
    null_const()}};
  auto result = eval(expr, events, all_rows(events), dh, reg);
  CHECK_EQUAL(std::move(dh).collect().size(), 0u);
  CHECK(is_null_array(result));
  CHECK_EQUAL(result.length(), events.length());
}

TEST("unary not handles mixed bool and null rows under a mask") {
  auto builder = ArrayBuilder<Record>{};
  builder.record().field("x").data(true);
  builder.record().field("x").null();
  builder.record().field("x").data(false);
  builder.record().field("x").data(std::int64_t{1});
  auto events = make_events(builder.finish());
  auto dh = tenzir::collecting_diagnostic_handler{};
  auto reg = tenzir::registry{};
  auto expr = tenzir::ast::expression{tenzir::ast::unary_expr{
    tenzir::located<tenzir::ast::unary_op>{tenzir::ast::unary_op::not_,
                                           tenzir::location::unknown},
    root_field("x")}};
  auto result = eval(expr, events, all_rows(events).keep_first(3), dh, reg);
  CHECK_EQUAL(std::move(dh).collect().size(), 0u);
  CHECK_EQUAL(materialize_legacy(result.get(0)), tenzir::data{false});
  CHECK_EQUAL(materialize_legacy(result.get(1)), tenzir::data{});
  CHECK_EQUAL(materialize_legacy(result.get(2)), tenzir::data{true});
}

TEST("unary not still rejects numeric operands") {
  auto events = make_int_field_events({0, 1});
  auto dh = tenzir::collecting_diagnostic_handler{};
  auto reg = tenzir::registry{};
  auto expr = tenzir::ast::expression{tenzir::ast::unary_expr{
    tenzir::located<tenzir::ast::unary_op>{tenzir::ast::unary_op::not_,
                                           tenzir::location::unknown},
    root_field("x")}};
  auto result = eval(expr, events, all_rows(events), dh, reg);
  CHECK_EQUAL(std::move(dh).collect().size(), 1u);
  CHECK(is_null_array(result));
}

TEST("unary neg kernel negates an int column") {
  auto events = make_int_field_events({1, -2, 3});
  auto dh = tenzir::null_diagnostic_handler{};
  auto reg = tenzir::registry{};
  auto expr = tenzir::ast::expression{tenzir::ast::unary_expr{
    tenzir::located<tenzir::ast::unary_op>{tenzir::ast::unary_op::neg,
                                           tenzir::location::unknown},
    root_field("x")}};

  auto result = eval(expr, events, all_rows(events), dh, reg);
  auto arr = as_int_array(result);
  REQUIRE(arr.has_value());
  REQUIRE_EQUAL(arr->length(), 3);
  CHECK_EQUAL(*arr->get(0), -1);
  CHECK_EQUAL(*arr->get(1), 2);
  CHECK_EQUAL(*arr->get(2), -3);
}

TEST("unary neg kernel converts uint to int and nulls overflow") {
  auto builder = ArrayBuilder<Record>{};
  constexpr auto limit = static_cast<UInt>(std::numeric_limits<Int>::max()) + 1;
  builder.record().field("x").data(std::uint64_t{3});
  builder.record().field("x").data(limit);
  builder.record().field("x").data(limit + 1);
  auto events = make_events(builder.finish());
  auto dh = tenzir::null_diagnostic_handler{};
  auto reg = tenzir::registry{};
  auto expr = tenzir::ast::expression{tenzir::ast::unary_expr{
    tenzir::located<tenzir::ast::unary_op>{tenzir::ast::unary_op::neg,
                                           tenzir::location::unknown},
    root_field("x")}};

  auto result = eval(expr, events, all_rows(events), dh, reg);
  auto result_union = as_union_array(result);
  REQUIRE(result_union.has_value());
  auto int_alt = result_union->get_alternative<Int>();
  REQUIRE(int_alt.is_some());
  CHECK(int_alt->present.get(0));
  CHECK(int_alt->present.get(1));
  CHECK(not int_alt->present.get(2));
  CHECK_EQUAL(*int_alt->data.get(0), -3);
  CHECK_EQUAL(*int_alt->data.get(1), std::numeric_limits<Int>::min());
  auto null_alt = result_union->get_alternative<Null>();
  REQUIRE(null_alt.is_some());
  CHECK(null_alt->present.get(2));
}

TEST("unary neg kernel nulls the minimum int") {
  auto events = make_int_field_events({std::numeric_limits<Int>::min(), 5});
  auto dh = tenzir::null_diagnostic_handler{};
  auto reg = tenzir::registry{};
  auto expr = tenzir::ast::expression{tenzir::ast::unary_expr{
    tenzir::located<tenzir::ast::unary_op>{tenzir::ast::unary_op::neg,
                                           tenzir::location::unknown},
    root_field("x")}};

  auto result = eval(expr, events, all_rows(events), dh, reg);
  auto result_union = as_union_array(result);
  REQUIRE(result_union.has_value());
  auto int_alt = result_union->get_alternative<Int>();
  REQUIRE(int_alt.is_some());
  CHECK(not int_alt->present.get(0));
  CHECK(int_alt->present.get(1));
  CHECK_EQUAL(*int_alt->data.get(1), -5);
  auto null_alt = result_union->get_alternative<Null>();
  REQUIRE(null_alt.is_some());
  CHECK(null_alt->present.get(0));
}

TEST("unary neg kernel negates durations") {
  auto builder = ArrayBuilder<Record>{};
  builder.record().field("x").data(tenzir::duration{std::chrono::seconds{2}});
  builder.record().field("x").data(
    tenzir::duration{std::numeric_limits<Duration::rep>::min()});
  auto events = make_events(builder.finish());
  auto dh = tenzir::null_diagnostic_handler{};
  auto reg = tenzir::registry{};
  auto expr = tenzir::ast::expression{tenzir::ast::unary_expr{
    tenzir::located<tenzir::ast::unary_op>{tenzir::ast::unary_op::neg,
                                           tenzir::location::unknown},
    root_field("x")}};

  auto result = eval(expr, events, all_rows(events), dh, reg);
  auto result_union = as_union_array(result);
  REQUIRE(result_union.has_value());
  auto duration_alt = result_union->get_alternative<Duration>();
  REQUIRE(duration_alt.is_some());
  CHECK(duration_alt->present.get(0));
  CHECK(not duration_alt->present.get(1));
  CHECK_EQUAL(*duration_alt->data.get(0),
              tenzir::duration{std::chrono::seconds{-2}});
  auto null_alt = result_union->get_alternative<Null>();
  REQUIRE(null_alt.is_some());
  CHECK(null_alt->present.get(1));
}

TEST("events report physical and active row counts") {
  auto all = make_int_field_events({1, 2, 3});
  CHECK_EQUAL(all.length(), 3);
  CHECK_EQUAL(all.active_count(), 3);
  auto mask_builder = storage::BitMap::Builder{};
  mask_builder.emplace_back(true);
  mask_builder.emplace_back(false);
  mask_builder.emplace_back(true);
  auto partial = Events{all.data, mask_builder.finish(), all.meta};
  CHECK_EQUAL(partial.length(), 3);
  CHECK_EQUAL(partial.active_count(), 2);
}

TEST("binary add kernel sums two int columns") {
  auto builder = ArrayBuilder<Record>{};
  builder.record().field("a").data(std::int64_t{1});
  builder.record().field("a").data(std::int64_t{10});
  auto lhs_data = builder.finish();
  auto lhs_events = make_events(lhs_data);
  auto dh = tenzir::null_diagnostic_handler{};
  auto reg = tenzir::registry{};
  // Both operands read from the same input column `a`, so `a + a` doubles
  // it; this avoids needing two separate input columns for the test.
  auto expr = tenzir::ast::expression{tenzir::ast::binary_expr{
    root_field("a"), tenzir::ast::binary_op::add, root_field("a")}};

  auto result = eval(expr, lhs_events, all_rows(lhs_events), dh, reg);
  auto arr = as_int_array(result);
  REQUIRE(arr.has_value());
  REQUIRE_EQUAL(arr->length(), 2);
  CHECK_EQUAL(*arr->get(0), 2);
  CHECK_EQUAL(*arr->get(1), 20);
}

TEST("structured values compare unequal to null") {
  auto events = make_int_field_events({1});
  auto dh = tenzir::null_diagnostic_handler{};
  auto reg = tenzir::registry{};
  auto check_comparison
    = [&](tenzir::ast::expression lhs, tenzir::ast::binary_op op,
          tenzir::ast::expression rhs, bool expected) {
        auto expr = tenzir::ast::expression{
          tenzir::ast::binary_expr{std::move(lhs), op, std::move(rhs)}};
        auto result = eval(expr, events, all_rows(events), dh, reg);
        auto values = as_bool_array(result);
        REQUIRE(values.has_value());
        REQUIRE_EQUAL(values->length(), 1);
        CHECK_EQUAL(*values->get(0), expected);
      };
  using enum tenzir::ast::binary_op;
  check_comparison(empty_list(), eq, null_const(), false);
  check_comparison(null_const(), eq, empty_list(), false);
  check_comparison(empty_list(), neq, null_const(), true);
  check_comparison(null_const(), neq, empty_list(), true);
  check_comparison(empty_record(), eq, null_const(), false);
  check_comparison(null_const(), eq, empty_record(), false);
  check_comparison(empty_record(), neq, null_const(), true);
  check_comparison(null_const(), neq, empty_record(), true);
}

TEST("structured union alternatives compare unequal to null") {
  auto builder = ArrayBuilder<Record>{};
  static_cast<void>(builder.record().field("x").list());
  builder.record().field("x").null();
  static_cast<void>(builder.record().field("x").record());
  auto data = builder.finish();
  auto events = make_events(data);
  auto dh = tenzir::null_diagnostic_handler{};
  auto reg = tenzir::registry{};
  auto expr = tenzir::ast::expression{tenzir::ast::binary_expr{
    root_field("x"), tenzir::ast::binary_op::eq, null_const()}};
  auto result = eval(expr, events, all_rows(events), dh, reg);
  auto values = as_bool_array(result);
  REQUIRE(values.has_value());
  REQUIRE_EQUAL(values->length(), 3);
  CHECK_EQUAL(*values->get(0), false);
  CHECK_EQUAL(*values->get(1), true);
  CHECK_EQUAL(*values->get(2), false);
}

TEST("unary neg kernel rejects a string column") {
  auto builder = ArrayBuilder<Record>{};
  builder.record().field("x").data(std::string_view{"hello"});
  auto data = builder.finish();
  auto events = make_events(data);
  auto dh = tenzir::null_diagnostic_handler{};
  auto reg = tenzir::registry{};
  auto expr = tenzir::ast::expression{tenzir::ast::unary_expr{
    tenzir::located<tenzir::ast::unary_op>{tenzir::ast::unary_op::neg,
                                           tenzir::location::unknown},
    root_field("x")}};

  auto result = eval(expr, events, all_rows(events), dh, reg);
  // Rejected type: the kernel is never invoked, but the row is now an
  // explicit null rather than absent.
  CHECK_EQUAL(result.length(), 1);
  CHECK(is_null_array(result));
}

TEST("unary neg kernel resolves a UnionArray operand per alternative") {
  auto builder = ArrayBuilder<Record>{};
  builder.record().field("x").data(std::int64_t{1});
  builder.record().field("x").data(2.5);
  auto data = builder.finish();
  auto events = make_events(data);
  auto dh = tenzir::null_diagnostic_handler{};
  auto reg = tenzir::registry{};
  // `x` is a mixed-type column (row 0: `Int`, row 1: `Float`), i.e. a
  // `UnionArray` once evaluated.
  auto expr = tenzir::ast::expression{tenzir::ast::unary_expr{
    tenzir::located<tenzir::ast::unary_op>{tenzir::ast::unary_op::neg,
                                           tenzir::location::unknown},
    root_field("x")}};

  auto result = eval(expr, events, all_rows(events), dh, reg);
  // Both alternatives are accepted by `neg`, so both result tags are
  // engaged: the result comes back as a `UnionArray` again.
  auto result_union = as_union_array(result);
  REQUIRE(result_union.has_value());
  REQUIRE_EQUAL(result_union->length(), 2);

  auto int_alt = result_union->get_alternative<Int>();
  REQUIRE(int_alt.is_some());
  CHECK(int_alt->present.get(0));
  CHECK(not int_alt->present.get(1));
  CHECK_EQUAL(*int_alt->data.get(0), -1);

  auto float_alt = result_union->get_alternative<Float>();
  REQUIRE(float_alt.is_some());
  CHECK(not float_alt->present.get(0));
  CHECK(float_alt->present.get(1));
  CHECK_EQUAL(*float_alt->data.get(1), -2.5);
}

TEST("unary neg kernel nulls unsupported alternatives inside a UnionArray") {
  auto builder = ArrayBuilder<Record>{};
  builder.record().field("x").data(std::int64_t{1});
  builder.record().field("x").data(std::string_view{"hello"});
  auto data = builder.finish();
  auto events = make_events(data);
  auto dh = tenzir::null_diagnostic_handler{};
  auto reg = tenzir::registry{};
  // `x` mixes `Int` (accepted by `neg`) with `String` (rejected by it).
  auto expr = tenzir::ast::expression{tenzir::ast::unary_expr{
    tenzir::located<tenzir::ast::unary_op>{tenzir::ast::unary_op::neg,
                                           tenzir::location::unknown},
    root_field("x")}};

  auto result = eval(expr, events, all_rows(events), dh, reg);
  // Both `Int` (a real value) and `Null` (the rejected `String` row) are
  // engaged, so the result comes back as a `UnionArray` of the two.
  auto result_union = as_union_array(result);
  REQUIRE(result_union.has_value());

  auto int_alt = result_union->get_alternative<Int>();
  REQUIRE(int_alt.is_some());
  CHECK(int_alt->present.get(0));
  CHECK(not int_alt->present.get(1));
  CHECK_EQUAL(*int_alt->data.get(0), -1);

  // The `String` row was rejected (with a diagnostic, same as a top-level
  // string column) and is now an explicit null rather than absent.
  auto null_alt = result_union->get_alternative<Null>();
  REQUIRE(null_alt.is_some());
  CHECK(not null_alt->present.get(0));
  CHECK(null_alt->present.get(1));
}

TEST("unary neg kernel over a UnionArray respects a narrower mask") {
  auto builder = ArrayBuilder<Record>{};
  builder.record().field("x").data(std::int64_t{1});
  builder.record().field("x").data(2.5);
  builder.record().field("x").data(std::int64_t{3});
  auto data = builder.finish();
  auto events = make_events(data);
  auto dh = tenzir::null_diagnostic_handler{};
  auto reg = tenzir::registry{};
  auto expr = tenzir::ast::expression{tenzir::ast::unary_expr{
    tenzir::located<tenzir::ast::unary_op>{tenzir::ast::unary_op::neg,
                                           tenzir::location::unknown},
    root_field("x")}};

  // Exclude row 0 (the `Int` alternative's first occurrence) via the mask.
  auto mask_builder = storage::BitMap::Builder{};
  mask_builder.emplace_back(false);
  mask_builder.emplace_back(true);
  mask_builder.emplace_back(true);
  auto result = eval(expr, events, std::move(mask_builder).finish(), dh, reg);

  auto result_union = as_union_array(result);
  REQUIRE(result_union.has_value());
  auto int_alt = result_union->get_alternative<Int>();
  REQUIRE(int_alt.is_some());
  CHECK(not int_alt->present.get(0));
  CHECK(int_alt->present.get(2));
  CHECK_EQUAL(*int_alt->data.get(2), -3);
}

TEST("binary in kernel checks substring containment for two string constants") {
  auto events = make_int_field_events({1});
  auto dh = tenzir::null_diagnostic_handler{};
  auto reg = tenzir::registry{};
  auto make_in_expr = [](std::string needle, std::string haystack) {
    auto lhs = tenzir::ast::expression{tenzir::ast::constant::make(
      tenzir::located<tenzir::data>{tenzir::data{std::move(needle)},
                                    tenzir::location::unknown})};
    auto rhs = tenzir::ast::expression{tenzir::ast::constant::make(
      tenzir::located<tenzir::data>{tenzir::data{std::move(haystack)},
                                    tenzir::location::unknown})};
    return tenzir::ast::expression{
      tenzir::ast::binary_expr{lhs, tenzir::ast::binary_op::in, rhs}};
  };

  auto contained
    = eval(make_in_expr("y", "xyz"), events, all_rows(events), dh, reg);
  auto contained_arr = as_bool_array(contained);
  REQUIRE(contained_arr.has_value());
  REQUIRE_EQUAL(contained_arr->length(), 1);
  CHECK_EQUAL(*contained_arr->get(0), true);

  auto missing
    = eval(make_in_expr("q", "xyz"), events, all_rows(events), dh, reg);
  auto missing_arr = as_bool_array(missing);
  REQUIRE(missing_arr.has_value());
  CHECK_EQUAL(*missing_arr->get(0), false);
}

TEST("root field read backfills null for a field missing from a row's shape") {
  // Row 0 and row 2 never call `.field("x")` at all, i.e. `x` is absent from
  // their shape entirely -- distinct from the mixed-type-but-always-present
  // case the other `UnionArray` tests above cover.
  auto builder = ArrayBuilder<Record>{};
  builder.record();
  builder.record().field("x").data(std::int64_t{2});
  builder.record();
  auto data = builder.finish();
  auto events = make_events(data);
  auto dh = tenzir::null_diagnostic_handler{};
  auto reg = tenzir::registry{};
  auto result = eval(root_field("x"), events, all_rows(events), dh, reg);

  // The result is present wherever the input mask is present. Missing fields
  // are represented in `.data`, which is a `UnionArray` of `Null` (the absent
  // rows) and `Int` (the one present row), not a plain `Array<Int>`.
  auto result_union = as_union_array(result);
  REQUIRE(result_union.has_value());
  auto int_alt = result_union->get_alternative<Int>();
  REQUIRE(int_alt.is_some());
  CHECK(not int_alt->present.get(0));
  CHECK(int_alt->present.get(1));
  CHECK(not int_alt->present.get(2));
  CHECK_EQUAL(*int_alt->data.get(1), 2);
}

TEST("nested field access masks values by the parent record alternative") {
  auto builder = ArrayBuilder<Record>{};
  builder.record().field("flow").record().field("src_ip").data(
    std::string_view{"192.0.2.1"});
  builder.record().field("flow").null();
  static_cast<void>(builder.record().field("flow").record());
  auto data = builder.finish();
  auto events = make_events(data);
  auto dh = tenzir::null_diagnostic_handler{};
  auto reg = tenzir::registry{};
  auto field = tenzir::ast::expression{tenzir::ast::field_access{
    root_field("flow"),
    tenzir::location::unknown,
    true,
    tenzir::ast::identifier{"src_ip", tenzir::location::unknown},
  }};
  auto expr = tenzir::ast::expression{tenzir::ast::binary_expr{
    std::move(field),
    tenzir::ast::binary_op::neq,
    tenzir::ast::expression{tenzir::ast::constant::make(
      tenzir::located<tenzir::data>{tenzir::data{caf::none},
                                    tenzir::location::unknown})},
  }};

  auto result = eval(expr, events, all_rows(events), dh, reg);
  auto values = as_bool_array(result);
  REQUIRE(values.has_value());
  CHECK_EQUAL(*values->get(0), true);
  CHECK_EQUAL(*values->get(1), false);
  CHECK_EQUAL(*values->get(2), false);
}

TEST("binary add kernel over a field absent from some rows' shape") {
  // Mirrors `select x = src_port | y = x + 42` where `src_port` is missing
  // from some JSON records' keys entirely: reading `x` back and adding a
  // constant must not read a never-written offset for the absent rows.
  // `null + int` isn't an accepted combination, so the absent row becomes an
  // explicit `Null` result (a warning plus `results.set_null`) rather than
  // propagating absence -- `result.present` is therefore all-true, and
  // `result` comes back as a `UnionArray` of `Int` and `Null`.
  auto builder = ArrayBuilder<Record>{};
  builder.record().field("x").data(std::int64_t{1});
  builder.record();
  builder.record().field("x").data(std::int64_t{3});
  auto data = builder.finish();
  auto events = make_events(data);
  auto dh = tenzir::null_diagnostic_handler{};
  auto reg = tenzir::registry{};
  auto expr = tenzir::ast::expression{tenzir::ast::binary_expr{
    root_field("x"), tenzir::ast::binary_op::add,
    tenzir::ast::expression{tenzir::ast::constant::make(
      tenzir::located<tenzir::data>{tenzir::data{std::int64_t{42}},
                                    tenzir::location::unknown})}}};

  auto result = eval(expr, events, all_rows(events), dh, reg);

  auto result_union = as_union_array(result);
  REQUIRE(result_union.has_value());
  auto int_alt = result_union->get_alternative<Int>();
  REQUIRE(int_alt.is_some());
  CHECK(int_alt->present.get(0));
  CHECK(not int_alt->present.get(1));
  CHECK(int_alt->present.get(2));
  CHECK_EQUAL(*int_alt->data.get(0), 43);
  CHECK_EQUAL(*int_alt->data.get(2), 45);

  auto null_alt = result_union->get_alternative<Null>();
  REQUIRE(null_alt.is_some());
  CHECK(null_alt->present.get(1));
}

namespace {

auto int_const(std::int64_t v) -> tenzir::ast::expression {
  return tenzir::ast::expression{tenzir::ast::constant::make(
    tenzir::located<tenzir::data>{tenzir::data{v}, tenzir::location::unknown})};
}

auto if_expr(tenzir::ast::expression then, tenzir::ast::expression cond)
  -> tenzir::ast::expression {
  return tenzir::ast::expression{tenzir::ast::binary_expr{
    std::move(then), tenzir::ast::binary_op::if_, std::move(cond)}};
}

auto else_expr(tenzir::ast::expression left, tenzir::ast::expression right)
  -> tenzir::ast::expression {
  return tenzir::ast::expression{tenzir::ast::binary_expr{
    std::move(left), tenzir::ast::binary_op::else_, std::move(right)}};
}

} // namespace

TEST("if without else nulls rows where the condition is false") {
  // `x if x > 1`, over `x = [1, 2, 3]`: row 0 fails the condition and becomes
  // `null`; rows 1 and 2 keep their `x` value.
  auto events = make_int_field_events({1, 2, 3});
  auto dh = tenzir::null_diagnostic_handler{};
  auto reg = tenzir::registry{};
  auto cond = tenzir::ast::expression{tenzir::ast::binary_expr{
    root_field("x"), tenzir::ast::binary_op::gt, int_const(1)}};
  auto expr = if_expr(root_field("x"), cond);

  auto result = eval(expr, events, all_rows(events), dh, reg);

  auto result_union = as_union_array(result);
  REQUIRE(result_union.has_value());
  REQUIRE_EQUAL(result_union->length(), 3);
  auto int_alt = result_union->get_alternative<Int>();
  REQUIRE(int_alt.is_some());
  CHECK(not int_alt->present.get(0));
  CHECK(int_alt->present.get(1));
  CHECK(int_alt->present.get(2));
  CHECK_EQUAL(*int_alt->data.get(1), 2);
  CHECK_EQUAL(*int_alt->data.get(2), 3);

  auto null_alt = result_union->get_alternative<Null>();
  REQUIRE(null_alt.is_some());
  CHECK(null_alt->present.get(0));
}

TEST("if else picks the fallback where the condition is false") {
  // `x if x > 1 else 0`, over `x = [1, 2, 3]`.
  auto events = make_int_field_events({1, 2, 3});
  auto dh = tenzir::null_diagnostic_handler{};
  auto reg = tenzir::registry{};
  auto cond = tenzir::ast::expression{tenzir::ast::binary_expr{
    root_field("x"), tenzir::ast::binary_op::gt, int_const(1)}};
  auto expr = else_expr(if_expr(root_field("x"), cond), int_const(0));

  auto result = eval(expr, events, all_rows(events), dh, reg);
  auto arr = as_int_array(result);
  REQUIRE(arr.has_value());
  REQUIRE_EQUAL(arr->length(), 3);
  CHECK_EQUAL(*arr->get(0), 0);
  CHECK_EQUAL(*arr->get(1), 2);
  CHECK_EQUAL(*arr->get(2), 3);
}

TEST("else alone substitutes the right side only where the left is null") {
  // `x else 0`, where `x` is absent from some rows' shape entirely, exercises
  // the plain `eval_else` path (the left side is a `root_field`, not a nested
  // `if_` binary_expr, so no short-circuit delegation to `eval_if` applies).
  auto builder = ArrayBuilder<Record>{};
  builder.record().field("x").data(std::int64_t{1});
  builder.record();
  builder.record().field("x").data(std::int64_t{3});
  auto data = builder.finish();
  auto events = make_events(data);
  auto dh = tenzir::null_diagnostic_handler{};
  auto reg = tenzir::registry{};
  auto expr = else_expr(root_field("x"), int_const(0));

  auto result = eval(expr, events, all_rows(events), dh, reg);
  auto arr = as_int_array(result);
  REQUIRE(arr.has_value());
  REQUIRE_EQUAL(arr->length(), 3);
  CHECK_EQUAL(*arr->get(0), 1);
  CHECK_EQUAL(*arr->get(1), 0);
  CHECK_EQUAL(*arr->get(2), 3);
}

TEST("if warns and treats a non-bool condition as false") {
  auto events = make_int_field_events({1, 2});
  auto dh = tenzir::collecting_diagnostic_handler{};
  auto reg = tenzir::registry{};
  // The condition is `x` itself (an `Int`), not a `Bool` expression.
  auto expr = if_expr(root_field("x"), root_field("x"));

  auto result = eval(expr, events, all_rows(events), dh, reg);
  CHECK_EQUAL(std::move(dh).collect().size(), 1u);

  // Every row is treated as `false` (the condition never matched `Bool`), so
  // with no explicit else, the whole result is `null`.
  CHECK(is_null_array(result));
}

TEST("if produces a union when then/else branches have different types") {
  // `x if x > 1 else "small"`, over `x = [1, 2]`: row 0 takes the `String`
  // fallback, row 1 keeps its `Int` value -- two different runtime types
  // merged into one `UnionArray` result.
  auto events = make_int_field_events({1, 2});
  auto dh = tenzir::null_diagnostic_handler{};
  auto reg = tenzir::registry{};
  auto cond = tenzir::ast::expression{tenzir::ast::binary_expr{
    root_field("x"), tenzir::ast::binary_op::gt, int_const(1)}};
  auto fallback = tenzir::ast::expression{tenzir::ast::constant::make(
    tenzir::located<tenzir::data>{tenzir::data{std::string{"small"}},
                                  tenzir::location::unknown})};
  auto expr = else_expr(if_expr(root_field("x"), cond), fallback);

  auto result = eval(expr, events, all_rows(events), dh, reg);
  auto result_union = as_union_array(result);
  REQUIRE(result_union.has_value());

  auto int_alt = result_union->get_alternative<Int>();
  REQUIRE(int_alt.is_some());
  CHECK(not int_alt->present.get(0));
  CHECK(int_alt->present.get(1));
  CHECK_EQUAL(*int_alt->data.get(1), 2);

  auto string_alt = result_union->get_alternative<String>();
  REQUIRE(string_alt.is_some());
  CHECK(string_alt->present.get(0));
  CHECK(not string_alt->present.get(1));
}

namespace {

auto uint_const(std::uint64_t v) -> tenzir::ast::expression {
  return tenzir::ast::expression{tenzir::ast::constant::make(
    tenzir::located<tenzir::data>{tenzir::data{v}, tenzir::location::unknown})};
}

auto float_const(double v) -> tenzir::ast::expression {
  return tenzir::ast::expression{tenzir::ast::constant::make(
    tenzir::located<tenzir::data>{tenzir::data{v}, tenzir::location::unknown})};
}

auto compare_expr(tenzir::ast::expression lhs, tenzir::ast::binary_op op,
                  tenzir::ast::expression rhs) -> tenzir::ast::expression {
  return tenzir::ast::expression{
    tenzir::ast::binary_expr{std::move(lhs), op, std::move(rhs)}};
}

/// `Events` with one field `x` holding an `Int`, a `UInt`, a `Float` and a
/// `null` -- one row per alternative a comparison has to resolve.
auto make_mixed_number_events() -> Events {
  auto builder = ArrayBuilder<Record>{};
  builder.record().field("x").data(std::int64_t{1});
  builder.record().field("x").data(std::uint64_t{3});
  builder.record().field("x").data(2.5);
  builder.record().field("x").null();
  return make_events(builder.finish());
}

/// Reads a comparison result whose rows are a mix of `Bool` and `null`,
/// returning one entry per row: `None` for a null row.
auto bool_or_null_rows(const Array<Data>& result, storage::Index length)
  -> std::vector<Option<bool>> {
  auto rows
    = std::vector<Option<bool>>(static_cast<std::size_t>(length), None{});
  if (auto plain = as_bool_array(result)) {
    for (auto i = storage::Index{0}; i < length; ++i) {
      rows[static_cast<std::size_t>(i)] = *plain->get(i);
    }
    return rows;
  }
  // A result that is null on every row never becomes a union.
  if (is_null_array(result)) {
    return rows;
  }
  auto values = as_union_array(result);
  REQUIRE(values.has_value());
  auto bool_alt = values->get_alternative<Bool>();
  if (bool_alt.is_some()) {
    for (auto i = storage::Index{0}; i < length; ++i) {
      if (bool_alt->present.get(i)) {
        rows[static_cast<std::size_t>(i)] = *bool_alt->data.get(i);
      }
    }
  }
  return rows;
}

} // namespace

TEST("ordering comparisons resolve mixed numeric alternatives per row") {
  // `x < 2` over `x = [1 (int), 3u (uint), 2.5 (float), null]`. The `float`
  // row is newly supported; the `null` row is rejected with one warning.
  auto events = make_mixed_number_events();
  auto dh = tenzir::collecting_diagnostic_handler{};
  auto reg = tenzir::registry{};
  auto expr
    = compare_expr(root_field("x"), tenzir::ast::binary_op::lt, int_const(2));

  auto result = eval(expr, events, all_rows(events), dh, reg);
  CHECK_EQUAL(std::move(dh).collect().size(), 1u);

  auto rows = bool_or_null_rows(result, events.length());
  REQUIRE_EQUAL(rows.size(), 4u);
  CHECK_EQUAL(rows[0], Option{true});  // 1 < 2
  CHECK_EQUAL(rows[1], Option{false}); // 3u < 2
  CHECK_EQUAL(rows[2], Option{false}); // 2.5 < 2
  CHECK_EQUAL(rows[3], None{});        // null < 2
}

TEST("comparisons against a float constant accept every numeric row") {
  auto events = make_mixed_number_events();
  auto dh = tenzir::collecting_diagnostic_handler{};
  auto reg = tenzir::registry{};
  auto expr = compare_expr(root_field("x"), tenzir::ast::binary_op::geq,
                           float_const(2.5));

  auto result = eval(expr, events, all_rows(events), dh, reg);
  CHECK_EQUAL(std::move(dh).collect().size(), 1u);

  auto rows = bool_or_null_rows(result, events.length());
  REQUIRE_EQUAL(rows.size(), 4u);
  CHECK_EQUAL(rows[0], Option{false}); // 1 >= 2.5
  CHECK_EQUAL(rows[1], Option{true});  // 3u >= 2.5
  CHECK_EQUAL(rows[2], Option{true});  // 2.5 >= 2.5
  CHECK_EQUAL(rows[3], None{});        // null >= 2.5
}

TEST("comparisons are exact across the signedness boundary") {
  // The built-in operators would convert `-1` to `UINT64_MAX` first, making
  // `-1 < 1u` false and `-1 == UINT64_MAX` true.
  auto builder = ArrayBuilder<Record>{};
  builder.record().field("x").data(std::int64_t{-1});
  auto events = make_events(builder.finish());
  auto dh = tenzir::collecting_diagnostic_handler{};
  auto reg = tenzir::registry{};
  auto check = [&](tenzir::ast::binary_op op, tenzir::ast::expression rhs,
                   bool expected) {
    auto result = eval(compare_expr(root_field("x"), op, std::move(rhs)),
                       events, all_rows(events), dh, reg);
    auto values = as_bool_array(result);
    REQUIRE(values.has_value());
    REQUIRE_EQUAL(values->length(), 1);
    CHECK_EQUAL(*values->get(0), expected);
  };
  using enum tenzir::ast::binary_op;
  check(lt, uint_const(1), true);
  check(leq, uint_const(1), true);
  check(gt, uint_const(1), false);
  check(geq, uint_const(1), false);
  check(eq, uint_const(18446744073709551615ULL), false);
  check(neq, uint_const(18446744073709551615ULL), true);
  CHECK_EQUAL(std::move(dh).collect().size(), 0u);
}

TEST("ordering comparisons warn on a null operand, equality does not") {
  auto events = make_int_field_events({1});
  auto reg = tenzir::registry{};
  auto check = [&](tenzir::ast::expression lhs, tenzir::ast::binary_op op,
                   tenzir::ast::expression rhs, Option<bool> expected,
                   std::size_t warnings) {
    auto dh = tenzir::collecting_diagnostic_handler{};
    auto result = eval(compare_expr(std::move(lhs), op, std::move(rhs)), events,
                       all_rows(events), dh, reg);
    CHECK_EQUAL(std::move(dh).collect().size(), warnings);
    auto rows = bool_or_null_rows(result, events.length());
    REQUIRE_EQUAL(rows.size(), 1u);
    CHECK_EQUAL(rows[0], expected);
  };
  using enum tenzir::ast::binary_op;
  check(root_field("x"), lt, null_const(), None{}, 1u);
  check(root_field("x"), leq, null_const(), None{}, 1u);
  check(root_field("x"), gt, null_const(), None{}, 1u);
  check(root_field("x"), geq, null_const(), None{}, 1u);
  check(null_const(), lt, root_field("x"), None{}, 1u);
  check(null_const(), gt, null_const(), None{}, 1u);
  check(null_const(), geq, null_const(), None{}, 1u);
  check(null_const(), leq, null_const(), None{}, 1u);
  check(root_field("x"), eq, null_const(), Option{false}, 0u);
  check(root_field("x"), neq, null_const(), Option{true}, 0u);
  check(null_const(), eq, null_const(), Option{true}, 0u);
  check(null_const(), neq, null_const(), Option{false}, 0u);
}

TEST("ordering comparisons still reject types that have no order") {
  auto builder = ArrayBuilder<Record>{};
  builder.record().field("x").data(std::string_view{"a"});
  auto events = make_events(builder.finish());
  auto reg = tenzir::registry{};
  auto rhs = [] {
    return tenzir::ast::expression{tenzir::ast::constant::make(
      tenzir::located<tenzir::data>{tenzir::data{std::string{"b"}},
                                    tenzir::location::unknown})};
  };

  auto eq_dh = tenzir::collecting_diagnostic_handler{};
  auto eq_result
    = eval(compare_expr(root_field("x"), tenzir::ast::binary_op::eq, rhs()),
           events, all_rows(events), eq_dh, reg);
  CHECK_EQUAL(std::move(eq_dh).collect().size(), 0u);
  auto eq_values = as_bool_array(eq_result);
  REQUIRE(eq_values.has_value());
  CHECK_EQUAL(*eq_values->get(0), false);

  auto lt_dh = tenzir::collecting_diagnostic_handler{};
  auto lt_result
    = eval(compare_expr(root_field("x"), tenzir::ast::binary_op::lt, rhs()),
           events, all_rows(events), lt_dh, reg);
  CHECK_EQUAL(std::move(lt_dh).collect().size(), 1u);
  CHECK(is_null_array(lt_result));
}

TEST("binary in kernel matches list elements across numeric types") {
  // Covers the numeric row of `list_contains`, which shares the comparison
  // matrix with `==`: `int` matches a `uint` or `float` element of equal
  // value, and the signedness boundary stays exact.
  auto builder = ArrayBuilder<Record>{};
  builder.record().field("x").data(std::int64_t{1});
  builder.record().field("x").data(std::int64_t{3});
  builder.record().field("x").data(std::int64_t{-1});
  builder.record().field("x").data(std::int64_t{9});
  auto events = make_events(builder.finish());
  auto dh = tenzir::collecting_diagnostic_handler{};
  auto reg = tenzir::registry{};
  auto list = tenzir::ast::expression{tenzir::ast::list{
    tenzir::location::unknown,
    {
      tenzir::ast::list::item{float_const(1.0)},
      tenzir::ast::list::item{uint_const(3)},
      tenzir::ast::list::item{uint_const(18446744073709551615ULL)},
    },
    tenzir::location::unknown}};
  auto expr = compare_expr(root_field("x"), tenzir::ast::binary_op::in, list);

  auto result = eval(expr, events, all_rows(events), dh, reg);
  CHECK_EQUAL(std::move(dh).collect().size(), 0u);
  auto values = as_bool_array(result);
  REQUIRE(values.has_value());
  REQUIRE_EQUAL(values->length(), 4);
  CHECK_EQUAL(*values->get(0), true);  // 1 matches the float 1.0
  CHECK_EQUAL(*values->get(1), true);  // 3 matches the uint 3
  CHECK_EQUAL(*values->get(2), false); // -1 is not UINT64_MAX
  CHECK_EQUAL(*values->get(3), false); // 9 matches nothing
}

namespace {

auto bool_const(bool v) -> tenzir::ast::expression {
  return tenzir::ast::expression{tenzir::ast::constant::make(
    tenzir::located<tenzir::data>{tenzir::data{v}, tenzir::location::unknown})};
}

auto logical_expr(tenzir::ast::expression lhs, tenzir::ast::binary_op op,
                  tenzir::ast::expression rhs) -> tenzir::ast::expression {
  return tenzir::ast::expression{
    tenzir::ast::binary_expr{std::move(lhs), op, std::move(rhs)}};
}

constexpr auto truth_values = std::array<Option<bool>, 3>{true, false, None{}};

/// `Events` with fields `l` and `r` covering every combination of `true`,
/// `false`, and `null`: `l` varies slowest, both in `truth_values` order.
auto make_truth_table_events() -> Events {
  auto builder = ArrayBuilder<Record>{};
  for (auto l : truth_values) {
    for (auto r : truth_values) {
      auto row = builder.record();
      if (l) {
        row.field("l").data(*l);
      } else {
        row.field("l").null();
      }
      if (r) {
        row.field("r").data(*r);
      } else {
        row.field("r").null();
      }
    }
  }
  return make_events(builder.finish());
}

/// Evaluates `l <op> r` over the truth table and compares row by row.
auto check_truth_table(tenzir::ast::binary_op op,
                       std::vector<Option<bool>> const& expected) -> void {
  auto events = make_truth_table_events();
  auto dh = tenzir::collecting_diagnostic_handler{};
  auto reg = tenzir::registry{};
  auto expr = logical_expr(root_field("l"), op, root_field("r"));

  auto result = eval(expr, events, all_rows(events), dh, reg);
  CHECK_EQUAL(std::move(dh).collect().size(), 0u);

  auto rows = bool_or_null_rows(result, events.length());
  REQUIRE_EQUAL(rows.size(), expected.size());
  for (auto i = std::size_t{0}; i < rows.size(); ++i) {
    CHECK(rows[i] == expected[i]);
  }
}

} // namespace

TEST("and follows three-valued logic") {
  // Rows: (T,T) (T,F) (T,N) (F,T) (F,F) (F,N) (N,T) (N,F) (N,N).
  check_truth_table(tenzir::ast::binary_op::and_,
                    {true, false, None{}, false, false, false, None{}, false,
                     None{}});
}

TEST("or follows three-valued logic") {
  check_truth_table(tenzir::ast::binary_op::or_,
                    {true, true, true, true, false, None{}, true, None{},
                     None{}});
}

TEST("and/or warn on a non-bool operand and treat it as null") {
  auto events = make_int_field_events({1, 2});
  auto reg = tenzir::registry{};
  {
    auto dh = tenzir::collecting_diagnostic_handler{};
    auto expr = logical_expr(root_field("x"), tenzir::ast::binary_op::and_,
                             bool_const(true));
    auto result = eval(expr, events, all_rows(events), dh, reg);
    CHECK_EQUAL(std::move(dh).collect().size(), 1u);
    CHECK(is_null_array(result));
  }
  {
    auto dh = tenzir::collecting_diagnostic_handler{};
    auto expr = logical_expr(root_field("x"), tenzir::ast::binary_op::or_,
                             bool_const(false));
    auto result = eval(expr, events, all_rows(events), dh, reg);
    CHECK_EQUAL(std::move(dh).collect().size(), 1u);
    CHECK(is_null_array(result));
  }
}

TEST("and/or short-circuit and skip the right operand on decided rows") {
  // `x` is an `Int`, so evaluating the right operand would warn.
  auto events = make_int_field_events({1, 2});
  auto reg = tenzir::registry{};
  {
    auto dh = tenzir::collecting_diagnostic_handler{};
    auto expr = logical_expr(bool_const(false), tenzir::ast::binary_op::and_,
                             root_field("x"));
    auto result = eval(expr, events, all_rows(events), dh, reg);
    CHECK_EQUAL(std::move(dh).collect().size(), 0u);
    auto rows = bool_or_null_rows(result, events.length());
    CHECK(rows == (std::vector<Option<bool>>{false, false}));
  }
  {
    auto dh = tenzir::collecting_diagnostic_handler{};
    auto expr = logical_expr(bool_const(true), tenzir::ast::binary_op::or_,
                             root_field("x"));
    auto result = eval(expr, events, all_rows(events), dh, reg);
    CHECK_EQUAL(std::move(dh).collect().size(), 0u);
    auto rows = bool_or_null_rows(result, events.length());
    CHECK(rows == (std::vector<Option<bool>>{true, true}));
  }
}

TEST("record spread over a union turns non-record rows into empty records") {
  // `{...x, y: 1}` where `x` is `{a: 1}`, `7`, and `{b: 2}`. The `Int` row
  // has nothing to spread, so it only gets `y`; it must not keep the absent
  // shape of the union's record alternative.
  auto builder = ArrayBuilder<Record>{};
  builder.record().field("x").record().field("a").data(std::int64_t{1});
  builder.record().field("x").data(std::int64_t{7});
  builder.record().field("x").record().field("b").data(std::int64_t{2});
  auto events = make_events(builder.finish());
  auto dh = tenzir::collecting_diagnostic_handler{};
  auto reg = tenzir::registry{};
  auto items = std::vector<tenzir::ast::record::item>{};
  items.emplace_back(
    tenzir::ast::spread{tenzir::location::unknown, root_field("x")});
  items.emplace_back(tenzir::ast::record::field{
    tenzir::ast::identifier{std::string{"y"}, tenzir::location::unknown},
    int_const(1)});
  auto expr = tenzir::ast::expression{tenzir::ast::record{
    tenzir::location::unknown, std::move(items), tenzir::location::unknown}};

  auto result = eval(expr, events, all_rows(events), dh, reg);
  // One warning for spreading the `Int` row.
  CHECK_EQUAL(std::move(dh).collect().size(), 1u);

  REQUIRE(result.try_as<Record>().is_some());
  CHECK_EQUAL(materialize_legacy(result.get(0)),
              (tenzir::data{tenzir::record{{"a", std::int64_t{1}},
                                           {"y", std::int64_t{1}}}}));
  CHECK_EQUAL(materialize_legacy(result.get(1)),
              (tenzir::data{tenzir::record{{"y", std::int64_t{1}}}}));
  CHECK_EQUAL(materialize_legacy(result.get(2)),
              (tenzir::data{tenzir::record{{"b", std::int64_t{2}},
                                           {"y", std::int64_t{1}}}}));
}

namespace {

auto index_expr(tenzir::ast::expression subject, tenzir::ast::expression index,
                bool optional = false) -> tenzir::ast::expression {
  return tenzir::ast::expression{tenzir::ast::index_expr{
    std::move(subject), tenzir::location::unknown, std::move(index),
    tenzir::location::unknown, optional, false}};
}

auto string_const(std::string value) -> tenzir::ast::expression {
  return tenzir::ast::expression{tenzir::ast::constant::make(
    tenzir::located<tenzir::data>{tenzir::data{std::move(value)},
                                  tenzir::location::unknown})};
}

auto bitmap_of(std::vector<bool> bits) -> storage::BitMap {
  auto builder = storage::BitMap::Builder{};
  for (auto bit : bits) {
    builder.emplace_back(bit);
  }
  return std::move(builder).finish();
}

} // namespace

TEST("numeric index selects list elements and warns out of bounds") {
  auto builder = ArrayBuilder<Record>{};
  {
    auto list = builder.record().field("xs").list();
    list.data(std::int64_t{1});
    list.data(std::int64_t{2});
    list.data(std::int64_t{3});
  }
  {
    auto list = builder.record().field("xs").list();
    list.data(std::int64_t{4});
  }
  builder.record().field("xs").null();
  auto events = make_events(builder.finish());
  auto reg = tenzir::registry{};
  auto dh = tenzir::collecting_diagnostic_handler{};
  auto first = eval(index_expr(root_field("xs"), int_const(0)), events,
                    all_rows(events), dh, reg);
  CHECK_EQUAL(materialize_legacy(first.get(0)),
              (tenzir::data{std::int64_t{1}}));
  CHECK_EQUAL(materialize_legacy(first.get(1)),
              (tenzir::data{std::int64_t{4}}));
  CHECK_EQUAL(materialize_legacy(first.get(2)), (tenzir::data{}));
  auto diags = std::move(dh).collect();
  REQUIRE_EQUAL(diags.size(), 1u);
  CHECK_EQUAL(diags[0].message, "cannot index into `null`");
  auto last = eval(index_expr(root_field("xs"), int_const(-2)), events,
                   bitmap_of({true, true, false}), dh, reg);
  CHECK_EQUAL(materialize_legacy(last.get(0)), (tenzir::data{std::int64_t{2}}));
  CHECK_EQUAL(materialize_legacy(last.get(1)), (tenzir::data{}));
  diags = std::move(dh).collect();
  REQUIRE_EQUAL(diags.size(), 1u);
  CHECK_EQUAL(diags[0].message, "list index out of bounds");
}

TEST("numeric index selects record fields by position") {
  auto builder = ArrayBuilder<Record>{};
  {
    auto record = builder.record().field("r").record();
    record.field("a").data(std::int64_t{1});
    record.field("b").data(std::int64_t{2});
  }
  {
    auto record = builder.record().field("r").record();
    record.field("b").data(std::int64_t{20});
    record.field("a").data(std::int64_t{10});
  }
  auto events = make_events(builder.finish());
  auto reg = tenzir::registry{};
  auto dh = tenzir::collecting_diagnostic_handler{};
  auto second = eval(index_expr(root_field("r"), int_const(1)), events,
                     all_rows(events), dh, reg);
  CHECK_EQUAL(materialize_legacy(second.get(0)),
              (tenzir::data{std::int64_t{2}}));
  CHECK_EQUAL(materialize_legacy(second.get(1)),
              (tenzir::data{std::int64_t{10}}));
  auto missing = eval(index_expr(root_field("r"), int_const(2)), events,
                      all_rows(events), dh, reg);
  CHECK_EQUAL(materialize_legacy(missing.get(0)), (tenzir::data{}));
  auto diags = std::move(dh).collect();
  REQUIRE_EQUAL(diags.size(), 1u);
  CHECK_EQUAL(diags[0].message, "index out of bounds");
}

TEST("record indexing specializes only active types and diagnoses nulls") {
  auto builder = ArrayBuilder<Record>{};
  {
    auto row = builder.record();
    row.field("r").record().field("a").data(std::int64_t{1});
    row.field("key").data(std::string_view{"a"});
  }
  {
    auto row = builder.record();
    row.field("r").record().field("a").data(std::int64_t{2});
    row.field("key").null();
  }
  {
    auto row = builder.record();
    row.field("r").null();
    row.field("key").data(std::string_view{"a"});
  }
  {
    auto row = builder.record();
    row.field("r").data(true);
    row.field("key").data(std::int64_t{0});
  }
  auto events = make_events(builder.finish());
  auto reg = tenzir::registry{};
  auto dh = tenzir::collecting_diagnostic_handler{};
  auto mask = bitmap_of({true, true, true, false});
  auto dynamic = eval(index_expr(root_field("r"), root_field("key")), events,
                      mask, dh, reg);
  CHECK_EQUAL(materialize_legacy(dynamic.get(0)),
              (tenzir::data{std::int64_t{1}}));
  CHECK_EQUAL(materialize_legacy(dynamic.get(1)), tenzir::data{});
  CHECK_EQUAL(materialize_legacy(dynamic.get(2)), tenzir::data{});
  auto diags = std::move(dh).collect();
  REQUIRE_EQUAL(diags.size(), 2u);
  CHECK_EQUAL(diags[0].message, "cannot index into `null`");
  CHECK_EQUAL(diags[1].message, "cannot use `null` as index");
  auto constant = eval(index_expr(root_field("r"), string_const("a")), events,
                       mask, dh, reg);
  CHECK_EQUAL(materialize_legacy(constant.get(0)),
              (tenzir::data{std::int64_t{1}}));
  CHECK_EQUAL(materialize_legacy(constant.get(1)),
              (tenzir::data{std::int64_t{2}}));
  CHECK_EQUAL(materialize_legacy(constant.get(2)), tenzir::data{});
  diags = std::move(dh).collect();
  REQUIRE_EQUAL(diags.size(), 1u);
  CHECK_EQUAL(diags[0].message, "cannot index into `null`");
  auto optional = eval(index_expr(root_field("r"), root_field("key"), true),
                       events, mask, dh, reg);
  CHECK_EQUAL(materialize_legacy(optional.get(0)),
              (tenzir::data{std::int64_t{1}}));
  CHECK(std::move(dh).collect().empty());
  auto missing
    = eval(index_expr(root_field("r"), string_const("missing"), true), events,
           mask, dh, reg);
  CHECK_EQUAL(materialize_legacy(missing.get(0)), tenzir::data{});
  CHECK(std::move(dh).collect().empty());
}

TEST("constant list indices handle unsigned and signed extremes") {
  auto builder = ArrayBuilder<Record>{};
  builder.record().field("xs").list().data(std::int64_t{42});
  builder.record().field("xs").null();
  auto events = make_events(builder.finish());
  auto reg = tenzir::registry{};
  auto dh = tenzir::collecting_diagnostic_handler{};
  auto first = eval(index_expr(root_field("xs"), uint_const(0), true), events,
                    all_rows(events), dh, reg);
  CHECK_EQUAL(materialize_legacy(first.get(0)),
              (tenzir::data{std::int64_t{42}}));
  CHECK_EQUAL(materialize_legacy(first.get(1)), tenzir::data{});
  for (auto index : {int_const(std::numeric_limits<int64_t>::min()),
                     uint_const(std::numeric_limits<uint64_t>::max())}) {
    auto result = eval(index_expr(root_field("xs"), index, true), events,
                       all_rows(events), dh, reg);
    CHECK_EQUAL(materialize_legacy(result.get(0)), tenzir::data{});
    CHECK_EQUAL(materialize_legacy(result.get(1)), tenzir::data{});
  }
  CHECK(std::move(dh).collect().empty());
}

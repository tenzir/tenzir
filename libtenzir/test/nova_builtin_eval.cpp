//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

/// Evaluation tests for the *builtin* nova functions, as opposed to
/// `nova_eval.cpp` (the core evaluator) and `nova_function_plugin.cpp`
/// (argument binding). These cover the rows-I-must-produce contract: a
/// builtin's result must hold a materialized value at every row of the
/// frame's mask, with rows it cannot compute becoming explicit `Null`s, and
/// a legitimate `null` input must propagate without a warning.
///
/// The builtins had no evaluation coverage before this file. Most assertions
/// here are behavior-preserving and were checked against the pre-`Array<Data>`
/// evaluator; the exceptions are the `is_null_at` checks, which are new by
/// construction. Those rows used to be *absent* -- excluded by an
/// `EvalResult::present` mask that no longer exists -- and are now explicit
/// `Null`s. That is the point of the change, not an accident of it.

#include "tenzir/diagnostics.hpp"
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

#include <cstdint>
#include <string>
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

auto str_const(std::string value) -> ast::expression {
  return ast::expression{ast::constant::make(
    located<data>{data{std::move(value)}, location::unknown})};
}

auto int_const(std::int64_t value) -> ast::expression {
  return ast::expression{
    ast::constant::make(located<data>{data{value}, location::unknown})};
}

auto this_expr() -> ast::expression {
  return ast::expression{ast::this_{location::unknown}};
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

auto bitmap(std::vector<bool> bits) -> storage::BitMap {
  auto builder = storage::BitMap::Builder{};
  for (auto bit : bits) {
    builder.emplace_back(bit);
  }
  return std::move(builder).finish();
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
  return match(
    data,
    [&](Array<Int> const& arr) -> Option<std::int64_t> {
      return *arr.get(i);
    },
    [&](UnionArray const& u) -> Option<std::int64_t> {
      auto alt = u.get_alternative<Int>();
      if (not alt or not alt->present.get(i)) {
        return None{};
      }
      return *alt->data.get(i);
    },
    [](auto const&) -> Option<std::int64_t> {
      return None{};
    });
}

/// The `int` elements of the list at row `i`, or nothing if that row is not
/// a list.
auto list_at(Array<Data> const& data, storage::Index i)
  -> Option<std::vector<std::int64_t>> {
  auto lists = data.get_alternative<List>();
  if (not lists or not lists->present.get(i)) {
    return None{};
  }
  auto result = std::vector<std::int64_t>{};
  for (auto v : lists->data.get(i)) {
    result.push_back(*as<RowView<Int>>(v));
  }
  return result;
}

auto bool_at(Array<Data> const& data, storage::Index i) -> Option<bool> {
  auto bools = data.get_alternative<Bool>();
  if (not bools or not bools->present.get(i)) {
    return None{};
  }
  return *bools->data.get(i);
}

/// Records whose field `x` is, per row: a record `{a: 1}`, a `null`, and an
/// `int` (a type error).
auto make_mixed_record_events() -> Events {
  auto builder = ArrayBuilder<Record>{};
  builder.record().field("x").record().field("a").data(std::int64_t{1});
  builder.record().field("x").null();
  builder.record().field("x").data(std::int64_t{7});
  return make_events(builder.finish());
}

/// Records whose field `x` is, per row: the string `"a,b"`, a `null`, and an
/// `int` (a type error).
auto make_mixed_string_events() -> Events {
  auto builder = ArrayBuilder<Record>{};
  builder.record().field("x").data(std::string_view{"a,b"});
  builder.record().field("x").null();
  builder.record().field("x").data(std::int64_t{7});
  return make_events(builder.finish());
}

/// Records whose field `xs` is, per row: a two-element list, a `null`, and an
/// `int` (a type error). Exercises the union path of the list builtins.
auto make_mixed_list_events() -> Events {
  auto builder = ArrayBuilder<Record>{};
  {
    auto rec = builder.record();
    auto list = rec.field("xs").list();
    list.data(std::int64_t{1});
    list.data(std::int64_t{2});
  }
  builder.record().field("xs").null();
  builder.record().field("xs").data(std::int64_t{7});
  return make_events(builder.finish());
}

} // namespace

TEST("length nulls a non-list row and propagates a null silently") {
  auto events = make_mixed_list_events();
  auto dh = collecting_diagnostic_handler{};
  auto result = eval(call("length", {root_field("xs")}), events,
                     storage::BitMap{3, true}, dh);

  CHECK_EQUAL(int_at(result, 0), Option{std::int64_t{2}});
  // A legitimate `null` propagates as a null length...
  CHECK(is_null_at(result, 1));
  // ...and so does a type error, but that one is reported.
  CHECK(is_null_at(result, 2));
  auto diags = std::move(dh).collect();
  REQUIRE_EQUAL(diags.size(), size_t{1});
  CHECK_EQUAL(diags[0].message, "expected `list`, got a different type");
}

TEST("length leaves rows outside the mask alone and still fills the mask") {
  auto events = make_mixed_list_events();
  auto dh = collecting_diagnostic_handler{};
  // Exclude the non-list row, so no diagnostic should be emitted at all.
  auto result = eval(call("length", {root_field("xs")}), events,
                     bitmap({true, true, false}), dh);

  CHECK_EQUAL(int_at(result, 0), Option{std::int64_t{2}});
  CHECK(is_null_at(result, 1));
  CHECK(std::move(dh).collect().empty());
}

TEST("length of a uniformly-list column is dense over the mask") {
  auto builder = ArrayBuilder<Record>{};
  for (auto n : {1, 2, 3}) {
    auto rec = builder.record();
    auto list = rec.field("xs").list();
    for (auto i = 0; i < n; ++i) {
      list.data(std::int64_t{i});
    }
  }
  auto data = builder.finish();
  auto events = make_events(data);
  auto dh = collecting_diagnostic_handler{};
  auto result = eval(call("length", {root_field("xs")}), events,
                     bitmap({true, false, true}), dh);

  CHECK_EQUAL(int_at(result, 0), Option{std::int64_t{1}});
  CHECK_EQUAL(int_at(result, 2), Option{std::int64_t{3}});
  CHECK(std::move(dh).collect().empty());
}

TEST("length of a field absent from some rows' shapes nulls those rows") {
  // Row 1 has no `xs` at all, so the root-field read backfills an explicit
  // `Null` and `length` must propagate it without a type warning.
  auto builder = ArrayBuilder<Record>{};
  {
    auto rec = builder.record();
    auto list = rec.field("xs").list();
    list.data(std::int64_t{5});
  }
  builder.record();
  auto data = builder.finish();
  auto events = make_events(data);
  auto dh = collecting_diagnostic_handler{};
  auto result = eval(call("length", {root_field("xs")}), events,
                     storage::BitMap{2, true}, dh);

  CHECK_EQUAL(int_at(result, 0), Option{std::int64_t{1}});
  CHECK(is_null_at(result, 1));
  // The absent field itself warns; `length` must not add a type error.
  for (auto const& diag : std::move(dh).collect()) {
    CHECK_NOT_EQUAL(diag.message, "expected `list`, got a different type");
  }
}

TEST("has over a record column is a dense bool") {
  auto builder = ArrayBuilder<Record>{};
  builder.record().field("x").data(std::int64_t{1});
  builder.record().field("y").data(std::int64_t{2});
  auto data = builder.finish();
  auto events = make_events(data);
  auto dh = collecting_diagnostic_handler{};
  auto result = eval(call("has", {this_expr(), str_const("x")}), events,
                     storage::BitMap{2, true}, dh);
  // Every row is a record, so there is nothing to null out.
  auto bools = match(
    result,
    [](Array<Bool> const& b) -> Option<storage::BitMap> {
      return as<storage::BitMap>(b.storage());
    },
    [](auto const&) -> Option<storage::BitMap> {
      return None{};
    });
  REQUIRE(bools.has_value());
  CHECK(bools->get(0));
  CHECK(not bools->get(1));
}

TEST("string renders every row of the mask, nulls included") {
  auto builder = ArrayBuilder<Record>{};
  builder.record().field("x").data(std::int64_t{42});
  builder.record().field("x").null();
  auto data = builder.finish();
  auto events = make_events(data);
  auto dh = collecting_diagnostic_handler{};
  auto result = eval(call("string", {root_field("x")}), events,
                     storage::BitMap{2, true}, dh);
  CHECK_EQUAL(result.length(), 2);
}

TEST("map materializes constant lists and broadcasts varying captures") {
  for (auto empty : {false, true}) {
    auto builder = ArrayBuilder<Record>{};
    for (auto x : {Int{10}, Int{20}, Int{30}}) {
      builder.record().field("capture").data(x);
    }
    auto values = empty ? List{} : List{Int{1}, Int{2}};
    auto input = builder.finish().with_field_overwrite(
      "xs", {repeat(Data{std::move(values)}, 3), storage::BitMap{3, true}});
    auto events = make_events(std::move(input));
    auto body = ast::expression{ast::binary_expr{
      root_field("element"), ast::binary_op::add, root_field("capture")}};
    auto lambda = ast::expression{
      ast::lambda_expr{ast::identifier{"element", location::unknown},
                       location::unknown, std::move(body)}};
    auto dh = collecting_diagnostic_handler{};
    auto result = eval(call("map", {root_field("xs"), std::move(lambda)}),
                       events, bitmap({true, false, true}), dh);
    CHECK_EQUAL(result.length(), 3);
    auto lists = result.get_alternative<List>();
    REQUIRE(lists);
    for (auto row : {storage::Index{0}, storage::Index{2}}) {
      auto list = lists->data.get(row);
      CHECK_EQUAL(list.length(), empty ? 0 : 2);
      if (not empty) {
        CHECK_EQUAL(*as<RowView<Int>>(list.get(0)), (row + 1) * 10 + 1);
        CHECK_EQUAL(*as<RowView<Int>>(list.get(1)), (row + 1) * 10 + 2);
      }
    }
    CHECK(std::move(dh).collect().empty());
  }
}

TEST("add extends lists, initializes nulls, and nulls a non-list row") {
  auto events = make_mixed_list_events();
  auto dh = collecting_diagnostic_handler{};
  auto result = eval(call("add", {root_field("xs"), int_const(3)}), events,
                     storage::BitMap{3, true}, dh);
  CHECK_EQUAL(list_at(result, 0), (std::vector<std::int64_t>{1, 2, 3}));
  CHECK_EQUAL(list_at(result, 1), (std::vector<std::int64_t>{3}));
  CHECK(is_null_at(result, 2));
  auto diags = std::move(dh).collect();
  REQUIRE_EQUAL(diags.size(), size_t{1});
  CHECK_EQUAL(diags[0].message, "expected `list`, got a different type");
}

TEST("add on a null column makes singleton lists without a warning") {
  auto builder = ArrayBuilder<Record>{};
  builder.record().field("xs").null();
  builder.record().field("xs").null();
  auto events = make_events(builder.finish());
  auto dh = collecting_diagnostic_handler{};
  auto result = eval(call("add", {root_field("xs"), int_const(42)}), events,
                     storage::BitMap{2, true}, dh);
  CHECK_EQUAL(list_at(result, 0), (std::vector<std::int64_t>{42}));
  CHECK_EQUAL(list_at(result, 1), (std::vector<std::int64_t>{42}));
  CHECK(std::move(dh).collect().empty());
}

TEST("add ignores a non-list row outside the mask") {
  auto events = make_mixed_list_events();
  auto dh = collecting_diagnostic_handler{};
  auto result = eval(call("add", {root_field("xs"), int_const(3)}), events,
                     bitmap({true, true, false}), dh);
  CHECK_EQUAL(list_at(result, 0), (std::vector<std::int64_t>{1, 2, 3}));
  CHECK_EQUAL(list_at(result, 1), (std::vector<std::int64_t>{3}));
  CHECK(std::move(dh).collect().empty());
}

TEST("has propagates null and nulls a non-record row with a warning") {
  auto events = make_mixed_record_events();
  auto dh = collecting_diagnostic_handler{};
  auto result = eval(call("has", {root_field("x"), str_const("a")}), events,
                     storage::BitMap{3, true}, dh);
  CHECK_EQUAL(bool_at(result, 0), Option{true});
  CHECK(is_null_at(result, 1));
  CHECK(is_null_at(result, 2));
  auto diags = std::move(dh).collect();
  REQUIRE_EQUAL(diags.size(), size_t{1});
  CHECK_EQUAL(diags[0].message, "expected `record`, got a different type");
}

TEST("has on a null column is null without a warning") {
  auto builder = ArrayBuilder<Record>{};
  builder.record().field("x").null();
  auto events = make_events(builder.finish());
  auto dh = collecting_diagnostic_handler{};
  auto result = eval(call("has", {root_field("x"), str_const("a")}), events,
                     storage::BitMap{1, true}, dh);
  CHECK(is_null_at(result, 0));
  CHECK(std::move(dh).collect().empty());
}

TEST("has on a non-record column warns with the type") {
  auto builder = ArrayBuilder<Record>{};
  builder.record().field("x").data(std::int64_t{1});
  auto events = make_events(builder.finish());
  auto dh = collecting_diagnostic_handler{};
  auto result = eval(call("has", {root_field("x"), str_const("a")}), events,
                     storage::BitMap{1, true}, dh);
  CHECK(is_null_at(result, 0));
  auto diags = std::move(dh).collect();
  REQUIRE_EQUAL(diags.size(), size_t{1});
  CHECK_EQUAL(diags[0].message, "expected `record`, got `int`");
}

TEST("split propagates null silently and warns once for a non-string row") {
  auto events = make_mixed_string_events();
  auto dh = collecting_diagnostic_handler{};
  auto result = eval(call("split", {root_field("x"), str_const(",")}), events,
                     storage::BitMap{3, true}, dh);
  auto lists = result.get_alternative<List>();
  REQUIRE(lists);
  REQUIRE(lists->present.get(0));
  auto pieces = lists->data.get(0);
  REQUIRE_EQUAL(pieces.length(), 2);
  CHECK_EQUAL(*as<RowView<std::string_view>>(pieces.get(0)), "a");
  CHECK_EQUAL(*as<RowView<std::string_view>>(pieces.get(1)), "b");
  CHECK(is_null_at(result, 1));
  CHECK(is_null_at(result, 2));
  auto diags = std::move(dh).collect();
  REQUIRE_EQUAL(diags.size(), size_t{1});
  CHECK_EQUAL(diags[0].message, "expected `string`, got a different type");
}

TEST("split does not warn when the non-string row is outside the mask") {
  auto events = make_mixed_string_events();
  auto dh = collecting_diagnostic_handler{};
  auto result = eval(call("split", {root_field("x"), str_const(",")}), events,
                     bitmap({true, true, false}), dh);
  CHECK(is_null_at(result, 1));
  CHECK(std::move(dh).collect().empty());
}

TEST("split on a null column is null without a warning") {
  auto builder = ArrayBuilder<Record>{};
  builder.record().field("x").null();
  auto events = make_events(builder.finish());
  auto dh = collecting_diagnostic_handler{};
  auto result = eval(call("split", {root_field("x"), str_const(",")}), events,
                     storage::BitMap{1, true}, dh);
  CHECK(is_null_at(result, 0));
  CHECK(std::move(dh).collect().empty());
}

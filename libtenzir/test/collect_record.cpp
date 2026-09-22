//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/diagnostics.hpp"
#include "tenzir/nova/aggregation.hpp"
#include "tenzir/nova/materialize.hpp"
#include "tenzir/test/test.hpp"
#include "tenzir/tql2/registry.hpp"

#include <initializer_list>
#include <string>
#include <utility>
#include <vector>

using namespace tenzir;
using namespace tenzir::nova;

namespace {

auto collect_record_call(bool separate) -> ast::expression {
  auto args = std::vector<ast::expression>{};
  args.emplace_back(ast::root_field{ast::identifier{"x", location::unknown}});
  if (separate) {
    args.emplace_back(ast::root_field{ast::identifier{"y", location::unknown}});
  }
  auto call = ast::function_call{
    ast::entity{{ast::identifier{"collect_record", location::unknown}}},
    std::move(args), location::unknown, false};
  call.fn.ref = entity_path{
    std::string{entity_pkg_std}, {"collect_record"}, entity_ns::fn};
  return ast::expression{std::move(call)};
}

auto make_collect_record(diagnostic_handler& dh, bool separate = false)
  -> Box<AggregationInstance> {
  auto reg = global_registry();
  auto result = AggregationInstance::make(collect_record_call(separate),
                                          InstantiateCtx{dh, *reg});
  REQUIRE(result);
  return std::move(*result);
}

auto events(std::initializer_list<Record> rows) -> Events {
  auto builder = ArrayBuilder<Data>{};
  for (auto const& row : rows) {
    append_data(builder, Data{row});
  }
  auto data = builder.finish().try_as<Record>();
  REQUIRE(data);
  auto length = data->length();
  return Events{std::move(*data), storage::BitMap{length, true},
                Events::Meta::make_empty(length)};
}

auto result_of(AggregationInstance const& instance) -> data {
  return materialize(RowView<Data>{instance.get()});
}

} // namespace

TEST("collect_record scalar calls preserve sparse rows and constant lists") {
  auto dh = collecting_diagnostic_handler{};
  auto reg = global_registry();
  auto evaluator
    = Evaluator::make(collect_record_call(false), InstantiateCtx{dh, *reg});
  REQUIRE(evaluator);
  auto input = events(
    {Record{{"x", Int{42}}}, Record{{"x", List{List{String{"a"}, Int{1}}}}},
     Record{{"x", Int{42}}}, Record{{"x", List{Record{{"b", Bool{true}}}}}},
     Record{{"x", Int{42}}}});
  auto mask = storage::BitMap::Builder{};
  for (auto bit : {false, true, false, true, false}) {
    mask.emplace_back(bit);
  }
  input.mask = std::move(mask).finish();
  auto result = evaluator->eval(input, EvalCtx{dh});
  CHECK_EQUAL(result.length(), input.length());
  CHECK_EQUAL(materialize(result.get(1)), (data{record{{"a", int64_t{1}}}}));
  CHECK_EQUAL(materialize(result.get(3)), (data{record{{"b", true}}}));
  input.data = input.data.with_field_overwrite(
    "x", {repeat(Data{List{List{String{"constant"}, Int{2}}}}, input.length()),
          storage::BitMap{input.length(), true}});
  result = evaluator->eval(input, EvalCtx{dh});
  CHECK_EQUAL(materialize(result.get(1)),
              (data{record{{"constant", int64_t{2}}}}));
  CHECK_EQUAL(materialize(result.get(3)),
              (data{record{{"constant", int64_t{2}}}}));
  CHECK(std::move(dh).collect().empty());
}

TEST("collect_record scalar calls align two columns and reset each row") {
  auto dh = collecting_diagnostic_handler{};
  auto reg = global_registry();
  auto evaluator
    = Evaluator::make(collect_record_call(true), InstantiateCtx{dh, *reg});
  REQUIRE(evaluator);
  auto input = events({Record{{"x", List{String{"a"}, String{"b"}}},
                              {"y", List{Int{1}, Bool{true}}}},
                       Record{{"x", List{}}, {"y", List{}}},
                       Record{{"x", List{String{"c"}}}, {"y", List{Null{}}}}});
  auto result = evaluator->eval(input, EvalCtx{dh});
  CHECK_EQUAL(materialize(result.get(0)),
              (data{record{{"a", int64_t{1}}, {"b", true}}}));
  CHECK_EQUAL(materialize(result.get(1)), (data{record{}}));
  CHECK_EQUAL(materialize(result.get(2)), (data{record{{"c", data{}}}}));
  CHECK(std::move(dh).collect().empty());
}

TEST("collect_record skips batches without usable list alternatives") {
  auto check_null = [](Record row, bool separate, size_t warnings) {
    auto dh = collecting_diagnostic_handler{};
    auto reg = global_registry();
    auto evaluator = Evaluator::make(collect_record_call(separate),
                                     InstantiateCtx{dh, *reg});
    REQUIRE(evaluator);
    auto result = evaluator->eval(events({std::move(row)}), EvalCtx{dh});
    CHECK_EQUAL(result.length(), 1);
    CHECK_EQUAL(materialize(result.get(0)), data{});
    auto diagnostics = std::move(dh).collect();
    CHECK_EQUAL(diagnostics.size(), warnings);
    for (auto const& diagnostic : diagnostics) {
      CHECK_EQUAL(diagnostic.message, "expected `list`");
    }
  };
  check_null(Record{{"x", Null{}}}, false, 0);
  check_null(Record{{"x", Int{42}}}, false, 1);
  check_null(Record{{"x", List{}}, {"y", Null{}}}, true, 0);
  check_null(Record{{"x", List{}}, {"y", Int{42}}}, true, 1);
  check_null(Record{{"x", Int{42}}, {"y", Null{}}}, true, 0);
  check_null(Record{{"x", Null{}}, {"y", Int{42}}}, true, 0);
  check_null(Record{{"x", Int{42}}, {"y", List{}}}, true, 1);
}

TEST("collect_record validates list masks once per argument") {
  auto dh = collecting_diagnostic_handler{};
  auto reg = global_registry();
  auto evaluator
    = Evaluator::make(collect_record_call(true), InstantiateCtx{dh, *reg});
  REQUIRE(evaluator);
  auto input = events({Record{{"x", Int{42}}, {"y", Int{42}}},
                       Record{{"x", Int{42}}, {"y", Null{}}},
                       Record{{"x", Null{}}, {"y", Int{42}}},
                       Record{{"x", List{}}, {"y", Int{42}}},
                       Record{{"x", List{}}, {"y", Int{42}}},
                       Record{{"x", Int{42}}, {"y", List{}}},
                       Record{{"x", Int{42}}, {"y", List{}}},
                       Record{{"x", List{String{"a"}}}, {"y", List{Int{1}}}},
                       Record{{"x", Int{42}}, {"y", Int{42}}}});
  auto mask = storage::BitMap::Builder{};
  for (auto bit : {false, true, true, true, true, true, true, true, false}) {
    mask.emplace_back(bit);
  }
  input.mask = std::move(mask).finish();
  auto result = evaluator->eval(input, EvalCtx{dh});
  CHECK_EQUAL(result.length(), input.length());
  for (auto row = storage::Index{1}; row < 7; ++row) {
    CHECK_EQUAL(materialize(result.get(row)), data{});
  }
  CHECK_EQUAL(materialize(result.get(7)), (data{record{{"a", int64_t{1}}}}));
  auto diagnostics = std::move(dh).collect();
  REQUIRE_EQUAL(diagnostics.size(), size_t{2});
  for (auto const& diagnostic : diagnostics) {
    CHECK_EQUAL(diagnostic.message, "expected `list`");
  }
  // The arrays still have list alternatives, but none of their rows are active.
  auto null_mask = storage::BitMap::Builder{};
  for (auto bit :
       {false, true, true, false, false, false, false, false, false}) {
    null_mask.emplace_back(bit);
  }
  input.mask = std::move(null_mask).finish();
  auto null_dh = collecting_diagnostic_handler{};
  result = evaluator->eval(input, EvalCtx{null_dh});
  CHECK_EQUAL(materialize(result.get(1)), data{});
  CHECK_EQUAL(materialize(result.get(2)), data{});
  CHECK(std::move(null_dh).collect().empty());
}

TEST("collect_record replaces duplicate values instead of merging them") {
  auto dh = collecting_diagnostic_handler{};
  auto reg = global_registry();
  auto evaluator
    = Evaluator::make(collect_record_call(false), InstantiateCtx{dh, *reg});
  REQUIRE(evaluator);
  auto input = events({Record{
    {"x", List{
            Record{{"a", List{Int{1}, Int{2}}},
                   {"nested", Record{{"old", Int{1}}}},
                   {"nullable", Int{1}}},
            List{String{"a"}, List{Int{3}}},
            Record{{"nested", Record{{"new", Int{2}}}}, {"nullable", Null{}}},
          }}}});
  auto result = evaluator->eval(input, EvalCtx{dh});
  CHECK_EQUAL(materialize(result.get(0)),
              (data{record{{"a", list{int64_t{3}}},
                           {"nested", record{{"new", int64_t{2}}}},
                           {"nullable", data{}}}}));
  CHECK(std::move(dh).collect().empty());
}

TEST("collect_record aggregates mixed entry shapes in encounter order") {
  auto dh = collecting_diagnostic_handler{};
  auto instance = make_collect_record(dh);
  CHECK_EQUAL(result_of(*instance), (data{record{}}));
  instance->update(
    events({Record{{"x", Record{{"a", Int{1}}, {"b", Int{2}}}}},
            Record{{"x", List{String{"a"}, String{"second"}}}},
            Record{{"x", Record{{"value", Bool{true}}, {"key", String{"a"}}}}},
            Record{{"x", Record{{"b", Null{}}}}}}),
    EvalCtx{dh});
  CHECK_EQUAL(result_of(*instance), (data{record{{"a", true}, {"b", data{}}}}));
  // An absent field in a later record must not erase an earlier field.
  instance->update(events({Record{{"x", Record{{"c", Int{3}}}}}}), EvalCtx{dh});
  CHECK_EQUAL(result_of(*instance),
              (data{record{{"a", true}, {"b", data{}}, {"c", int64_t{3}}}}));
  CHECK(std::move(dh).collect().empty());
}

TEST("collect_record honors masks and resets its state") {
  auto dh = collecting_diagnostic_handler{};
  auto instance = make_collect_record(dh);
  auto input
    = events({Record{{"x", List{String{"a"}, Int{1}}}}, Record{{"x", Int{42}}},
              Record{{"x", List{String{"a"}, Int{3}}}}});
  auto mask = storage::BitMap::Builder{};
  mask.emplace_back(true);
  mask.emplace_back(false);
  mask.emplace_back(true);
  input.mask = std::move(mask).finish();
  instance->update(input, EvalCtx{dh});
  auto snapshot = result_of(*instance);
  CHECK_EQUAL(snapshot, (data{record{{"a", int64_t{3}}}}));
  input.mask = storage::BitMap{input.length(), false};
  instance->update(input, EvalCtx{dh});
  CHECK_EQUAL(result_of(*instance), snapshot);
  instance->reset();
  CHECK_EQUAL(result_of(*instance), (data{record{}}));
  instance->update(events({Record{{"x", Record{{"b", Int{2}}}}}}), EvalCtx{dh});
  CHECK_EQUAL(result_of(*instance), (data{record{{"b", int64_t{2}}}}));
  CHECK_EQUAL(snapshot, (data{record{{"a", int64_t{3}}}}));
  CHECK(std::move(dh).collect().empty());
}

TEST("collect_record owns values across batches and merges shallowly") {
  auto dh = collecting_diagnostic_handler{};
  auto instance = make_collect_record(dh);
  instance->update(
    events({Record{{"x", Record{{"nested", Record{{"old", Int{1}}}},
                                {"kept", List{String{"text"}, Int{2}}}}}}}),
    EvalCtx{dh});
  instance->update(events({Record{{"x", List{String{"nested"},
                                             Record{{"new", Bool{true}}}}}}}),
                   EvalCtx{dh});
  CHECK_EQUAL(result_of(*instance),
              (data{record{{"nested", record{{"new", true}}},
                           {"kept", list{std::string{"text"}, int64_t{2}}}}}));
  CHECK(std::move(dh).collect().empty());
}

TEST("collect_record accepts separate key and value expressions") {
  auto dh = collecting_diagnostic_handler{};
  auto instance = make_collect_record(dh, true);
  instance->update(
    events({Record{{"x", String{"a.b"}}, {"y", Int{1}}},
            Record{{"x", String{""}}, {"y", List{Bool{true}, Int{2}}}},
            Record{{"x", Null{}}, {"y", Int{9}}}}),
    EvalCtx{dh});
  instance->update(events({Record{{"x", String{"a.b"}}, {"y", Null{}}}}),
                   EvalCtx{dh});
  CHECK_EQUAL(result_of(*instance), (data{record{{"a.b", data{}},
                                                 {"", list{true, int64_t{2}}},
                                                 {"null", int64_t{9}}}}));
  CHECK(std::move(dh).collect().empty());
}

TEST("collect_record stringifies aggregation keys before resolving "
     "duplicates") {
  for (auto separate : {false, true}) {
    auto dh = collecting_diagnostic_handler{};
    auto instance = make_collect_record(dh, separate);
    auto input
      = separate
          ? events({Record{{"x", Int{42}}, {"y", Int{1}}},
                    Record{{"x", Bool{false}}, {"y", Int{2}}},
                    Record{{"x", Null{}}, {"y", Int{3}}}})
          : events(
              {Record{{"x", List{Int{42}, Int{1}}}},
               Record{{"x", Record{{"key", Bool{false}}, {"value", Int{2}}}}},
               Record{{"x", List{Null{}, Int{3}}}}});
    instance->update(input, EvalCtx{dh});
    auto snapshot = result_of(*instance);
    CHECK_EQUAL(snapshot, (data{record{{"42", int64_t{1}},
                                       {"false", int64_t{2}},
                                       {"null", int64_t{3}}}}));
    auto more = separate
                  ? events({Record{{"x", String{"42"}}, {"y", Bool{true}}},
                            Record{{"x", String{"false"}}, {"y", Null{}}},
                            Record{{"x", String{"null"}}, {"y", List{Int{4}}}}})
                  : events({Record{{"x", Record{{"42", Bool{true}}}}},
                            Record{{"x", List{String{"false"}, Null{}}}},
                            Record{{"x", Record{{"key", String{"null"}},
                                                {"value", List{Int{4}}}}}}});
    instance->update(more, EvalCtx{dh});
    CHECK_EQUAL(
      result_of(*instance),
      (data{
        record{{"42", true}, {"false", data{}}, {"null", list{int64_t{4}}}}}));
    CHECK_EQUAL(snapshot, (data{record{{"42", int64_t{1}},
                                       {"false", int64_t{2}},
                                       {"null", int64_t{3}}}}));
    CHECK(std::move(dh).collect().empty());
  }
}

TEST("collect_record reserves only the exact key and value shape") {
  auto dh = collecting_diagnostic_handler{};
  auto instance = make_collect_record(dh);
  instance->update(
    events({Record{{"x", Record{{"key", String{"a"}}, {"value", Int{1}}}}},
            Record{{"x", Record{{"key", String{"literal"}},
                                {"value", Int{2}},
                                {"extra", Bool{true}}}}}}),
    EvalCtx{dh});
  CHECK_EQUAL(result_of(*instance),
              (data{record{{"a", int64_t{1}},
                           {"key", std::string{"literal"}},
                           {"value", int64_t{2}},
                           {"extra", true}}}));
  CHECK(std::move(dh).collect().empty());
}

TEST("collect_record skips invalid entries with bounded diagnostics") {
  auto dh = collecting_diagnostic_handler{};
  auto instance = make_collect_record(dh);
  instance->update(
    events({Record{{"x", Null{}}}, Record{{"x", Record{}}},
            Record{{"x", List{}}}, Record{{"x", List{String{"a"}}}},
            Record{{"x", Int{1}}}, Record{{"x", Bool{true}}},
            Record{{"x", List{Int{1}, Int{2}}}},
            Record{{"x", Record{{"key", Bool{false}}, {"value", Int{3}}}}},
            Record{{"x", List{Null{}, Int{4}}}},
            Record{{"x", List{String{"valid"}, Int{5}}}}}),
    EvalCtx{dh});
  CHECK_EQUAL(result_of(*instance), (data{record{{"1", int64_t{2}},
                                                 {"false", int64_t{3}},
                                                 {"null", int64_t{4}},
                                                 {"valid", int64_t{5}}}}));
  auto diagnostics = std::move(dh).collect();
  REQUIRE_EQUAL(diagnostics.size(), size_t{2});
  for (auto const& diagnostic : diagnostics) {
    CHECK_EQUAL(diagnostic.severity, severity::warning);
  }
}

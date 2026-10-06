//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause
#include "clickhouse/prepare_slice.hpp"

#include <tenzir/series_builder.hpp>
#include <tenzir/test/test.hpp>
#include <tenzir/view3.hpp>

#include <arrow/array.h>
#include <arrow/record_batch.h>

#include <limits>

using namespace tenzir;
using namespace tenzir::plugins::clickhouse;

namespace {

auto destination(std::string name, std::string type,
                 std::vector<std::string> path) -> transformer_record {
  auto dh = collecting_diagnostic_handler{};
  auto result = transformer_record{};
  auto selector = path_type{name};
  result.transformations.emplace(
    name,
    make_functions_from_clickhouse(selector, type, dh, MappingMode::lossless));
  result.mapping_paths.emplace(name, std::move(path));
  selector = {"extra"};
  result.transformations.emplace(
    "extra", make_functions_from_clickhouse(selector, "JSON", dh,
                                            MappingMode::lossless));
  result.catch_all = "extra";
  result.found_column.resize(2);
  return result;
}

auto json(table_slice const& slice, std::string_view field) -> std::string {
  for (auto const& column : columns_of(slice)) {
    if (column.name == field) {
      return std::string{as<arrow::StringArray>(column.array).GetView(0)};
    }
  }
  return "missing";
}

} // namespace

TEST("reshaping extracts dotted paths and keeps the nested remainder") {
  auto builder = series_builder{};
  auto event = builder.record();
  auto endpoint = event.field("src").record();
  endpoint.field("port").data(uint64_t{443});
  endpoint.field("name").data("web");
  event.field("extra").data("input");
  auto input = builder.finish_as_table_slice("test").front();
  auto tr = destination("src.port", "UInt32", {"src", "port"});
  auto result = restructure_for_catch_all(input, tr);
  auto columns = series{result}.as<record_type>();
  CHECK_EQUAL(columns->type.field(0).name, "src.port");
  CHECK_EQUAL(columns->type.field(1).name, "extra");
  CHECK(is<record_type>(columns->type.field(1).type));
  auto dh = collecting_diagnostic_handler{};
  auto prepared = prepare_slice(result, tr, dh, {});
  CHECK_EQUAL(json(prepared, "extra"),
              R"({"src":{"name":"web"},"extra":"input"})");
  CHECK(dh.empty());
}

TEST("reshaping does not move incompatible mapped values to the remainder") {
  auto builder = series_builder{};
  auto event = builder.record();
  event.field("n").data("not a number");
  event.field("unknown").data("keep");
  auto input = builder.finish_as_table_slice("test").front();
  auto tr = destination("n", "UInt32", {"n"});
  auto result = restructure_for_catch_all(input, tr);
  auto columns = series{result}.as<record_type>();
  CHECK(is<string_type>(columns->type.field(0).type));
  CHECK_EQUAL(as<arrow::StringArray>(*columns->array->field(0)).GetView(0),
              "not a number");
  auto dh = collecting_diagnostic_handler{};
  auto prepared = prepare_slice(result, tr, dh, {});
  CHECK_EQUAL(json(prepared, "extra"), R"({"unknown":"keep"})");
  CHECK(dh.empty());
}

TEST("JSON printing omits null fields and retains empty records") {
  auto builder = series_builder{};
  auto event = builder.record();
  event.field("n").data(uint64_t{1});
  event.field("absent").null();
  event.field("empty").record();
  auto input = builder.finish_as_table_slice("test").front();
  auto tr = destination("n", "UInt32", {"n"});
  auto dh = collecting_diagnostic_handler{};
  auto result = prepare_slice(restructure_for_catch_all(input, tr), tr, dh, {});
  CHECK_EQUAL(json(result, "extra"), R"({"empty":{}})");
}

TEST("ordinary JSON printing removes nested nulls and list elements") {
  auto builder = series_builder{};
  auto payload = builder.record().field("payload").record();
  payload.field("nested").record().field("absent").null();
  auto items = payload.field("items").list();
  items.null();
  items.data(int64_t{1});
  items.null();
  auto input = builder.finish_as_table_slice("test").front();
  auto tr = destination("payload", "JSON", {"payload"});
  auto dh = collecting_diagnostic_handler{};
  auto result = prepare_slice(restructure_for_catch_all(input, tr), tr, dh, {});
  CHECK_EQUAL(json(result, "payload"), R"({"nested":{},"items":[1]})");
  CHECK(dh.empty());
}

TEST("extracting all children removes only the emptied parent") {
  auto builder = series_builder{};
  builder.record().field("src").record().field("port").data(uint64_t{1});
  auto input = builder.finish_as_table_slice("test").front();
  auto tr = destination("src.port", "UInt32", {"src", "port"});
  auto dh = collecting_diagnostic_handler{};
  auto result = prepare_slice(restructure_for_catch_all(input, tr), tr, dh, {});
  CHECK_EQUAL(json(result, "extra"), "{}");
}

TEST("reshaping sliced arrays shares value buffers and propagates null "
     "parents") {
  auto builder = series_builder{};
  builder.record().field("src").record().field("port").data(uint64_t{7});
  builder.record().field("src").null();
  builder.record().field("src").record().field("port").data(uint64_t{9});
  auto inputs = builder.finish_as_table_slice("test");
  auto input = concatenate(inputs);
  auto sliced = subslice(input, 1, 3);
  auto tr = destination("src.port", "Nullable(UInt32)", {"src", "port"});
  auto result = restructure_for_catch_all(sliced, tr);
  auto columns = series{result}.as<record_type>();
  CHECK_EQUAL(result.rows(), 2u);
  CHECK(columns->array->field(0)->IsNull(0));
  CHECK_EQUAL(as<arrow::UInt64Array>(*columns->array->field(0)).Value(1), 9u);
  auto root = series{input}.as<record_type>();
  auto source = as<arrow::StructArray>(*root->array->field(0)).field(0);
  CHECK_EQUAL(columns->array->field(0)->data()->buffers[1].get(),
              source->data()->buffers[1].get());
}

TEST("JSON strings pass unchanged through preparation") {
  auto builder = series_builder{};
  builder.record().field("payload").data("{invalid");
  auto input = builder.finish_as_table_slice("test").front();
  auto tr = destination("payload", "JSON", {"payload"});
  auto dh = collecting_diagnostic_handler{};
  auto result = prepare_slice(restructure_for_catch_all(input, tr), tr, dh, {});
  CHECK_EQUAL(json(result, "payload"), "{invalid");
  CHECK_EQUAL(json(result, "extra"), "{}");
  CHECK(dh.empty());
}

TEST("checked numeric validation ignores already rejected rows") {
  auto builder = uint64_type::make_arrow_builder(arrow_memory_pool());
  REQUIRE(builder->Append(256).ok());
  REQUIRE(builder->Append(42).ok());
  auto array = finish(*builder);
  auto path = path_type{"n"};
  auto dh = collecting_diagnostic_handler{};
  auto tr
    = make_functions_from_clickhouse(path, "UInt8", dh, MappingMode::lossless);
  auto mask = dropmask_type{true, false};
  CHECK(tr->update_dropmask(path, tenzir::type{uint64_type{}}, *array, mask, dh)
        == transformer::drop::none);
  CHECK(dh.empty());
  CHECK_EQUAL(mask, (dropmask_type{true, false}));
}

namespace {

auto defaulted_destination(std::string_view type) -> transformer_record {
  auto dh = collecting_diagnostic_handler{};
  auto result = transformer_record{};
  auto selector = path_type{"id"};
  result.transformations.emplace(
    "id",
    make_functions_from_clickhouse(selector, "Int64", dh, MappingMode::legacy));
  selector = {"d"};
  auto d
    = make_functions_from_clickhouse(selector, type, dh, MappingMode::legacy);
  d->has_default = true;
  result.transformations.emplace("d", std::move(d));
  result.found_column.resize(2);
  return result;
}

auto names(table_slice const& slice) -> std::vector<std::string> {
  auto result = std::vector<std::string>{};
  for (auto const& column : columns_of(slice)) {
    result.emplace_back(column.name);
  }
  return result;
}

} // namespace

TEST("nulls for a non-nullable column with a default omit the column") {
  auto builder = series_builder{};
  auto row = builder.record();
  row.field("id").data(int64_t{1});
  row.field("d").data(int64_t{5});
  row = builder.record();
  row.field("id").data(int64_t{2});
  row.field("d").null();
  row = builder.record();
  row.field("id").data(int64_t{3});
  row.field("d").data(int64_t{6});
  auto input = builder.finish_assert_one_slice("test");
  auto parts = split_null_defaults(input, defaulted_destination("Int64"));
  REQUIRE_EQUAL(parts.size(), size_t{2});
  CHECK_EQUAL(parts[0].rows, (std::vector<int64_t>{0, 2}));
  CHECK_EQUAL(names(parts[0].slice), (std::vector<std::string>{"id", "d"}));
  CHECK_EQUAL(parts[0].slice.rows(), uint64_t{2});
  CHECK_EQUAL(parts[1].rows, (std::vector<int64_t>{1}));
  CHECK_EQUAL(names(parts[1].slice), (std::vector<std::string>{"id"}));
  auto id = series{parts[1].slice}.as<record_type>()->array->field(0);
  CHECK_EQUAL(as<arrow::Int64Array>(*id).Value(0), 2);
}

TEST("an untyped null column with a default is omitted for all rows") {
  auto builder = series_builder{};
  builder.record().field("id").data(int64_t{1});
  builder.record().field("id").data(int64_t{2});
  auto ids = builder.finish_assert_one_slice("test");
  auto input = series{ids}.as<record_type>();
  auto nulls = std::make_shared<arrow::NullArray>(2);
  auto fields = std::vector<record_type::field_view>{
    {"id", int64_type{}},
    {"d", null_type{}},
  };
  auto schema = type{"test", record_type{fields}};
  auto batch = arrow::RecordBatch::Make(
    schema.to_arrow_schema(), 2,
    arrow::ArrayVector{input->array->field(0), nulls});
  auto parts = split_null_defaults(table_slice{batch, schema},
                                   defaulted_destination("Int64"));
  REQUIRE_EQUAL(parts.size(), size_t{1});
  CHECK(parts[0].rows.empty());
  CHECK_EQUAL(names(parts[0].slice), (std::vector<std::string>{"id"}));
  CHECK_EQUAL(parts[0].slice.rows(), uint64_t{2});
}

TEST("nulls for a list column with a default omit the column") {
  auto builder = series_builder{};
  auto row = builder.record();
  row.field("id").data(int64_t{1});
  row.field("d").list().data(int64_t{5});
  row = builder.record();
  row.field("id").data(int64_t{2});
  row.field("d").null();
  auto input = builder.finish_assert_one_slice("test");
  auto parts = split_null_defaults(
    input, defaulted_destination("Array(Nullable(Int64))"));
  REQUIRE_EQUAL(parts.size(), size_t{2});
  CHECK_EQUAL(parts[0].rows, (std::vector<int64_t>{0}));
  CHECK_EQUAL(names(parts[0].slice), (std::vector<std::string>{"id", "d"}));
  CHECK_EQUAL(parts[1].rows, (std::vector<int64_t>{1}));
  CHECK_EQUAL(names(parts[1].slice), (std::vector<std::string>{"id"}));
}

TEST("nulls stay for nullable columns and columns without a default") {
  auto builder = series_builder{};
  auto row = builder.record();
  row.field("id").data(int64_t{1});
  row.field("d").null();
  auto input = builder.finish_assert_one_slice("test");
  auto nullable
    = split_null_defaults(input, defaulted_destination("Nullable(Int64)"));
  REQUIRE_EQUAL(nullable.size(), size_t{1});
  CHECK_EQUAL(names(nullable[0].slice), (std::vector<std::string>{"id", "d"}));
  auto tr = defaulted_destination("Int64");
  tr.transfrom_and_index_for("d").trafo->has_default = false;
  auto required = split_null_defaults(input, tr);
  REQUIRE_EQUAL(required.size(), size_t{1});
  CHECK_EQUAL(names(required[0].slice), (std::vector<std::string>{"id", "d"}));
}

//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause
#include "clickhouse/easy_client.hpp"

#include "clickhouse/client.h"
#include "tenzir/detail/enumerate.hpp"
#include "tenzir/tql2/eval.hpp"
#include "tenzir/view3.hpp"

#include <algorithm>
#include <ranges>

using namespace clickhouse;

namespace tenzir::plugins::clickhouse {

auto easy_client::make(arguments args, operator_control_plane& ctrl)
  -> std::unique_ptr<easy_client> {
  return std::make_unique<easy_client>(std::move(args), ctrl, ctor_token{});
}

auto easy_client::remote_check_table_exists(std::string_view table_name)
  -> bool {
  /// We expect this query to invoke our callback 3 times
  /// * with a correctly typed, but 0 row block
  /// * with a correctly typed, 1 row block that contains the value we care about
  /// * with an empty block (end signal)
  auto query = Query{fmt::format("EXISTS TABLE {}", table_name)};
  auto exists = false;
  auto blocks = 0;
  auto cb = [&](const Block& block) {
    ++blocks;
    if (block.GetColumnCount() == 0) {
      TENZIR_ASSERT_EQ(blocks, 3);
      return;
    }
    TENZIR_ASSERT_EQ(block.GetColumnCount(), 1);
    auto cast = block[0]->As<ColumnUInt8>();
    TENZIR_ASSERT(cast);
    if (cast->Size() == 0) {
      TENZIR_ASSERT_EQ(blocks, 1);
      return;
    }
    exists = cast->At(0) == 1;
    TENZIR_ASSERT_EQ(blocks, 2);
  };
  query.OnData(cb);
  client_.Execute(query);
  return exists;
}

auto easy_client::remote_fetch_schema_transformations(
  std::string_view table_name) -> transformer_record* {
  TENZIR_ASSERT_EXPENSIVE(not transformations_.contains(table_name));
  auto query = Query{fmt::format("DESCRIBE TABLE {} "
                                 "SETTINGS describe_compact_output=1",
                                 table_name)};
  auto transformations = transformer_record{};
  bool failed = false;
  auto cb = [&](const Block& block) {
    auto path = path_type{};
    for (size_t i = 0; i < block.GetRowCount(); ++i) {
      auto name = block[0]->As<ColumnString>()->At(i);
      auto type_str = remove_non_significant_whitespace(
        block[1]->As<ColumnString>()->At(i));
      path.push_back(name);
      auto functions = make_functions_from_clickhouse(path, type_str, dh_);
      path.pop_back();
      if (not functions) {
        failed = true;
        return;
      }
      transformations.transformations.try_emplace(std::string{name},
                                                  std::move(functions));
    }
  };
  query.OnData(cb);
  client_.Execute(query);
  if (failed) {
    return nullptr;
  }
  transformations.found_column.resize(transformations.transformations.size(),
                                      false);
  auto [it, _] = transformations_.try_emplace(std::string{table_name},
                                              std::move(transformations));
  return &it.value();
}

auto easy_client::remote_create_table(const tenzir::record_type& schema,
                                      std::string_view table_name)
  -> failure_or<transformer_record*> {
  TENZIR_ASSERT(args_.primary);
  auto primary_found = false;
  auto path = path_type{};
  /// TODO: This should really be merged with the transformer itself. Its an
  /// (almost) duplicate of `make_record_functions_from_clickhouse`
  for (auto [k, t] : schema.fields()) {
    if (not validate_identifier(k)) {
      emit_invalid_identifier("column name", k, args_.operator_location, dh_);
      return failure::promise();
    }
    const auto is_primary = k == args_.primary->inner;
    path.push_back(k);
    TRY(auto clickhouse_typename,
        type_to_clickhouse_typename(path, t, not is_primary, dh_));
    TENZIR_ASSERT(not clickhouse_typename.empty());
    path.pop_back();
    primary_found |= is_primary;
  }
  if (not primary_found) {
    diagnostic::error(
      "cannot create table: primary key does not exist in input")
      .primary(*args_.primary, "column `{}` does not exist",
               args_.primary->inner)
      .emit(dh_);
    return failure::promise();
  }
  constexpr static std::string_view engine = "MergeTree";
  TRY(auto clickhouse_columns,
      plain_clickhouse_tuple_elements(path, schema, dh_, args_.primary->inner));
  const auto creation_modifier
    = args_.mode.inner == mode::create_append ? "IF NOT EXISTS" : "";
  auto query_text
    = fmt::format("CREATE TABLE {} {}"
                  " {}"
                  " ENGINE = {}"
                  " ORDER BY {}",
                  creation_modifier, table_name, clickhouse_columns, engine,
                  args_.primary->inner);
  auto query = Query{query_text};
  client_.Execute(query);
  auto* transformations = remote_fetch_schema_transformations(table_name);
  if (not transformations) {
    return failure::promise();
  }
  return transformations;
}

auto easy_client::ensure_transformations(const tenzir::record_type& schema,
                                         std::string_view table_name)
  -> transformer_record* {
  if (auto it = transformations_.find(table_name);
      it != transformations_.end()) {
    return &it.value();
  }
  // Note that technically, we have a ToCToU bug here. The table could be
  // created or deleted in between this, the `get` call below and the potential
  // creation in `insert`.
  const auto table_existed = remote_check_table_exists(table_name);
  TENZIR_TRACE("table exists: {}", table_existed);
  if (args_.mode.inner == mode::create and table_existed) {
    diagnostic::error("mode is `create`, but table `{}` already exists",
                      table_name)
      .primary(args_.mode)
      .primary(args_.table)
      .emit(dh_);
    return nullptr;
  }
  if (args_.mode.inner == mode::create_append and not table_existed
      and not args_.primary) {
    diagnostic::error("table `{}` does not exist, but no `primary` was "
                      "specified",
                      table_name)
      .primary(args_.table)
      .emit(dh_);
    return nullptr;
  }
  if (args_.mode.inner == mode::append and not table_existed) {
    diagnostic::error("mode is `append`, but table `{}` does not exist",
                      table_name)
      .primary(args_.mode)
      .primary(args_.table)
      .emit(dh_);
    return nullptr;
  }
  if (table_existed) {
    if (auto* transformations
        = remote_fetch_schema_transformations(table_name)) {
      return transformations;
    }
    return nullptr;
  }
  TENZIR_DEBUG("creating table `{}`", table_name);
  auto transformations = remote_create_table(schema, table_name);
  if (not transformations) {
    return nullptr;
  }
  TENZIR_DEBUG("created table `{}`", table_name);
  return *transformations;
}

auto easy_client::insert(const table_slice& slice, std::string_view table_name)
  -> bool {
  const auto& schema = as<record_type>(slice.schema());
  auto* transformations = ensure_transformations(schema, table_name);
  if (not transformations) {
    return false;
  }
  dropmask_.clear();
  dropmask_.resize(slice.rows());
  std::ranges::fill(transformations->found_column, false);
  auto updated = transformer::drop::none;
  path_type path{};
  /// TODO: This should really be merged with the transformer itself. Its an
  /// (almost) duplicate of `make_record_functions_from_clickhouse`
  for (const auto& [k, t, arr] : columns_of(slice)) {
    auto [trafo, idx] = transformations->transfrom_and_index_for(k);
    if (not trafo) {
      diagnostic::warning("column `{}` does not exist in the ClickHouse table",
                          k)
        .note("column will be dropped")
        .primary(args_.operator_location)
        .emit(dh_);
      continue;
    }
    transformations->found_column[idx] = true;
    path.push_back(k);
    updated = updated | trafo->update_dropmask(path, t, arr, dropmask_, dh_);
    path.pop_back();
    if (updated == transformer::drop::all) {
      // has already been reported
      return false;
    }
  }
  for (const auto& [i, kvp] :
       detail::enumerate(transformations->transformations)) {
    if (transformations->found_column[i]) {
      continue;
    }
    if (kvp.second->clickhouse_nullable) {
      continue;
    }
    diagnostic::warning(
      "required column missing in input, event will be dropped")
      .note("column `{}` is missing", kvp.first)
      .emit(dh_);
    return false;
  }
  const auto dropcount = pop_count(dropmask_);
  auto block = ::clickhouse::Block{};
  for (const auto& [k, t, arr] : columns_of(slice)) {
    const auto [trafo, out_idx] = transformations->transfrom_and_index_for(k);
    if (not trafo) {
      continue;
    }
    path.push_back(k);
    auto this_column
      = trafo->create_column(path, t, arr, dropmask_, dropcount, dh_);
    TENZIR_ASSERT(this_column->Size() == slice.rows() - dropcount,
                  "wrong row count in column `{}`; {} != {} - {}",
                  fmt::join(path, "."), this_column->Size(), slice.rows(),
                  dropcount);
    path.pop_back();
    if (not this_column) {
      diagnostic::warning("failed to add column `{}` to ClickHouse table", k)
        .emit(dh_);
      return false;
    }
    block.AppendColumn(std::string{k}, std::move(this_column));
  }
  TENZIR_ASSERT(block.GetRowCount() == slice.rows() - dropcount,
                "wrong row count for final block `{} != {} - {}`",
                block.GetRowCount(), slice.rows(), dropcount);
  if (block.GetRowCount() > 0 and block.GetColumnCount() > 0) {
    client_.Insert(std::string{table_name}, block);
  }
  return true;
}

auto easy_client::insert(const table_slice& slice) -> bool {
  auto ok = true;
  const auto table_values = eval(args_.table, slice, dh_);
  auto begin = int64_t{0};
  for (const auto& tables : table_values.parts()) {
    auto strings = tables.as<string_type>();
    if (not strings) {
      diagnostic::warning("expected `string`, got `{}`", tables.type.kind())
        .primary(args_.table)
        .note("event is skipped")
        .emit(dh_);
      begin += tables.length();
      continue;
    }
    const auto& array = *strings->array;
    auto run_begin = int64_t{-1};
    auto run_table = std::string_view{};
    auto flush = [&](int64_t rel_end) {
      if (run_begin == -1) {
        return;
      }
      const auto abs_begin = begin + run_begin;
      const auto abs_end = begin + rel_end;
      ok = insert(subslice(slice, static_cast<size_t>(abs_begin),
                           static_cast<size_t>(abs_end)),
                  run_table)
           and ok;
      run_begin = -1;
      run_table = {};
    };
    for (auto i = int64_t{0}; i < array.length(); ++i) {
      if (array.IsNull(i)) {
        flush(i);
        diagnostic::warning("expected `string`, got `null`")
          .primary(args_.table)
          .note("event is skipped")
          .emit(dh_);
        continue;
      }
      const auto table_name = array.GetView(i);
      if (not validate_table_name(table_name, args_.table.get_location(),
                                  dh_)) {
        flush(i);
        diagnostic::warning("invalid table `{}`", table_name)
          .primary(args_.table)
          .note("event is skipped")
          .hint("table must either be a quoted string, or match the regular "
                "expression `{}`; for database-qualified table names, both "
                "parts must satisfy this requirement",
                validation_expr)
          .emit(dh_);
        continue;
      }
      if (run_begin == -1) {
        run_begin = i;
        run_table = table_name;
        continue;
      }
      if (run_table != table_name) {
        flush(i);
        run_begin = i;
        run_table = table_name;
      }
    }
    flush(array.length());
    begin += array.length();
  }
  TENZIR_ASSERT(static_cast<size_t>(begin) == slice.rows());
  return ok;
}

void easy_client::ping() {
  client_.Ping();
}
} // namespace tenzir::plugins::clickhouse

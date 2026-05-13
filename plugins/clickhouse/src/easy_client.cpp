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

#include <fmt/format.h>

#include <algorithm>
#include <ranges>

using namespace clickhouse;

namespace tenzir::plugins::clickhouse {

auto easy_client::make(arguments args, const caf::actor_system_config& cfg,
                       diagnostic_handler& dh) -> std::shared_ptr<easy_client> {
  return std::make_shared<easy_client>(std::move(args), cfg, dh, ctor_token{});
}

auto easy_client::effective_table_name(std::string_view table_name) const
  -> std::string {
  if (not args_.default_database
      or table_name_quoting.split_at_unquoted(table_name, '.')) {
    return std::string{table_name};
  }
  return fmt::format(
    "{}.{}", quote_identifier_component(*args_.default_database), table_name);
}

auto easy_client::remote_check_exists(std::string_view object_kind,
                                      std::string_view object_name)
  -> failure_or<bool> {
  auto query = Query{fmt::format("EXISTS {} {}", object_kind, object_name)};
  auto exists = Option<bool>{};
  auto ok = true;
  auto emit_unexpected = [&](std::string_view note) {
    diagnostic::error("unexpected clickhouse response")
      .note("when checking for existence of {} `{}`", object_kind, object_name)
      .note("{}", note)
      .emit(dh_);
  };
  auto cb = [&](const Block& block) {
    if (not ok) {
      return;
    }
    if (block.GetColumnCount() == 0) {
      return;
    }
    if (block.GetColumnCount() != 1) {
      emit_unexpected("block should have exactly one column");
      ok = false;
      return;
    }
    auto cast = block[0]->As<ColumnUInt8>();
    if (not cast) {
      emit_unexpected("expected uint8 column");
      ok = false;
      return;
    }
    if (cast->Size() == 0) {
      return;
    }
    if (cast->Size() != 1 or block.GetRowCount() != 1) {
      emit_unexpected("expected exactly one row in the data block");
      ok = false;
      return;
    }
    if (exists) {
      emit_unexpected("expected exactly one data block");
      ok = false;
      return;
    }
    exists = cast->At(0) == 1;
  };
  query.OnData(cb);
  client_.Execute(query);
  if (not ok) {
    return failure::promise();
  }
  if (not exists) {
    emit_unexpected(
      "expected exactly one block with one uint8 column and one row");
    return failure::promise();
  }
  return *exists;
}

auto easy_client::remote_create_database(std::string_view database_name)
  -> void {
  auto query
    = Query{fmt::format("CREATE DATABASE IF NOT EXISTS {}", database_name)};
  client_.Execute(query);
}

auto easy_client::remote_fetch_schema_transformations(
  std::string_view table_name) -> failure_or<transformer_record*> {
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
    return failure::promise();
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
      emit_invalid_identifier<true>("column name", k, args_.operator_location,
                                    dh_);
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
  // Auto-generated CREATE TABLE queries for wide schemas can exceed the
  // server's default max_query_size (256 KiB). Remove the limit for this
  // statement, as the query text is derived from the inferred schema.
  query.SetSetting("max_query_size", {"0", QuerySettingsField::IMPORTANT});
  client_.Execute(query);
  return remote_fetch_schema_transformations(table_name);
}

auto easy_client::ensure_transformations(const tenzir::record_type& schema,
                                         std::string_view table_name)
  -> failure_or<transformer_record*> {
  if (auto it = transformations_.find(table_name);
      it != transformations_.end()) {
    return &it.value();
  }
  auto qualified_database = Option<std::string_view>{};
  if (auto split
      = split_table_name<true>(table_name, args_.table.get_location(), dh_)) {
    qualified_database = split->database;
    if (not qualified_database and args_.default_database) {
      qualified_database = std::string_view{*args_.default_database};
    }
  } else {
    return failure::promise();
  }
  if (qualified_database) {
    TRY(const auto database_existed,
        remote_check_exists("DATABASE", *qualified_database));
    TENZIR_TRACE("database exists: {}", database_existed);
    if (not database_existed) {
      if (args_.mode.inner == mode::append) {
        diagnostic::error("mode is `append`, but database `{}` does not exist",
                          *qualified_database)
          .primary(args_.mode)
          .primary(args_.table)
          .emit(dh_);
        return failure::promise();
      }
      TENZIR_DEBUG("creating database `{}`", *qualified_database);
      remote_create_database(*qualified_database);
      TENZIR_DEBUG("created database `{}`", *qualified_database);
    }
  }
  // Note that technically, we have a ToCToU bug here. The table could be
  // created or deleted in between this, the `get` call below and the potential
  // creation in `insert`.
  TRY(const auto table_existed, remote_check_exists("TABLE", table_name));
  TENZIR_TRACE("table exists: {}", table_existed);
  if (args_.mode.inner == mode::create and table_existed) {
    diagnostic::error("mode is `create`, but table `{}` already exists",
                      table_name)
      .primary(args_.mode)
      .primary(args_.table)
      .emit(dh_);
    return failure::promise();
  }
  if (args_.mode.inner == mode::create_append and not table_existed
      and not args_.primary) {
    diagnostic::error("table `{}` does not exist, but no `primary` was "
                      "specified",
                      table_name)
      .primary(args_.table)
      .emit(dh_);
    return failure::promise();
  }
  if (args_.mode.inner == mode::append and not table_existed) {
    diagnostic::error("mode is `append`, but table `{}` does not exist",
                      table_name)
      .primary(args_.mode)
      .primary(args_.table)
      .emit(dh_);
    return failure::promise();
  }
  if (table_existed) {
    return remote_fetch_schema_transformations(table_name);
  }
  return remote_create_table(schema, table_name);
}

auto easy_client::insert(const table_slice& slice, std::string_view table_name,
                         std::string_view query_id) -> failure_or<void> {
  auto resolved_table_name = effective_table_name(table_name);
  const auto& schema = as<record_type>(slice.schema());
  TRY(auto transformations,
      ensure_transformations(schema, resolved_table_name));
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
      return {};
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
    return {};
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
    path.pop_back();
    if (not this_column) {
      diagnostic::warning("failed to add column `{}` to ClickHouse table", k)
        .emit(dh_);
      return {};
    }
    TENZIR_ASSERT(this_column->Size() == slice.rows() - dropcount,
                  "wrong row count in column `{}`; {} != {} - {}",
                  fmt::join(path, "."), this_column->Size(), slice.rows(),
                  dropcount);
    block.AppendColumn(std::string{k}, std::move(this_column));
  }
  TENZIR_ASSERT(block.GetRowCount() == slice.rows() - dropcount,
                "wrong row count for final block `{} != {} - {}`",
                block.GetRowCount(), slice.rows(), dropcount);
  if (block.GetRowCount() > 0 and block.GetColumnCount() > 0) {
    if (query_id.empty()) {
      client_.Insert(std::string{resolved_table_name}, block);
    } else {
      client_.Insert(std::string{resolved_table_name}, std::string{query_id},
                     block);
    }
  }
  return {};
}

auto easy_client::insert_dynamic(const table_slice& slice,
                                 std::string_view query_id)
  -> failure_or<void> {
  auto guard = std::scoped_lock{client_mutex_};
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
    auto flush = [&](int64_t rel_end) -> failure_or<void> {
      if (run_begin == -1) {
        return {};
      }
      const auto abs_begin = begin + run_begin;
      const auto abs_end = begin + rel_end;
      TRY(insert(subslice(slice, static_cast<size_t>(abs_begin),
                          static_cast<size_t>(abs_end)),
                 run_table, query_id));
      run_begin = -1;
      run_table = {};
      return {};
    };
    for (auto i = int64_t{0}; i < array.length(); ++i) {
      if (array.IsNull(i)) {
        TRY(flush(i));
        diagnostic::warning("expected `string`, got `null`")
          .primary(args_.table)
          .note("event is skipped")
          .emit(dh_);
        continue;
      }
      const auto table_name = array.GetView(i);
      if (not validate_table_name<false>(table_name, args_.table.get_location(),
                                         dh_)) {
        TRY(flush(i));
        continue;
      }
      if (run_begin == -1) {
        run_begin = i;
        run_table = table_name;
        continue;
      }
      if (run_table != table_name) {
        TRY(flush(i));
        run_begin = i;
        run_table = table_name;
      }
    }
    TRY(flush(array.length()));
    begin += array.length();
  }
  TENZIR_ASSERT(static_cast<size_t>(begin) == slice.rows());
  return {};
}

void easy_client::ping() {
  auto guard = std::scoped_lock{client_mutex_};
  client_.Ping();
}
} // namespace tenzir::plugins::clickhouse

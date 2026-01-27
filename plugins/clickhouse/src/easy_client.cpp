//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause
#include "clickhouse/easy_client.hpp"

#include "clickhouse/client.h"
#include "tenzir/detail/enumerate.hpp"
#include "tenzir/view3.hpp"

#include <boost/regex.hpp>

#include <ranges>

using namespace clickhouse;
using namespace std::string_view_literals;

namespace tenzir::plugins::clickhouse {

auto easy_client::make(arguments args, operator_control_plane& ctrl)
  -> std::unique_ptr<easy_client> {
  auto client
    = std::make_unique<easy_client>(std::move(args), ctrl, ctor_token{});
  /// Note that technically, we have a ToCToU bug here. The table could be
  /// created or deleted in between this, the `get` call below and the potential
  /// creation in `insert`.
  const auto table_existed = client->check_if_table_exists();
  TENZIR_TRACE("table exists: {}", table_existed);
  if (client->args_.mode.inner == mode::create and table_existed) {
    diagnostic::error("mode is `create`, but table `{}` already exists",
                      client->args_.table.inner)
      .primary(client->args_.mode)
      .primary(client->args_.table)
      .emit(client->dh_);
    return nullptr;
  }
  if (client->args_.mode.inner == mode::create_append and not table_existed
      and not client->args_.primary) {
    diagnostic::error("table `{}` does not exist, but no `primary` was "
                      "specified",
                      client->args_.table.inner)
      .primary(client->args_.table)
      .emit(client->dh_);
    return nullptr;
  }
  if (client->args_.mode.inner == mode::append and not table_existed) {
    diagnostic::error("mode is `append`, but table `{}` does not exist",
                      client->args_.table.inner)
      .primary(client->args_.mode)
      .primary(client->args_.table)
      .emit(client->dh_);
    return nullptr;
  }
  if (table_existed) {
    if (not client->get_schema_transformations()) {
      return nullptr;
    }
  }
  return client;
}

auto easy_client::check_if_table_exists() -> bool {
  // // This does not work for some reason. It returns a table with 0 rows.
  // auto query = Query{fmt::format("EXISTS TABLE {}", table)};
  // auto exists = false;
  // auto cb = [&](const Block& block) {
  //   TENZIR_ASSERT(block.GetColumnCount() == 1);
  //   auto cast = block[0]->As<ColumnUInt8>();
  //   TENZIR_ASSERT(cast);
  //   exists = cast->At(0) == 1;
  // };
  // query.OnData(cb);
  // client.Execute(query);
  // return exists;
  auto query = Query{fmt::format("SHOW TABLES LIKE '{}'", args_.table.inner)};
  auto exists = false;
  auto cb = [&](const Block& block) {
    for (size_t i = 0; i < block.GetRowCount(); ++i) {
      auto name = block[0]->As<ColumnString>()->At(i);
      if (name == args_.table.inner) {
        exists = true;
        break;
      }
    }
  };
  query.OnData(cb);
  client_.Execute(query);
  return exists;
}

auto easy_client::get_schema_transformations() -> failure_or<void> {
  auto query = Query{fmt::format("DESCRIBE TABLE {} "
                                 "SETTINGS describe_compact_output=1",
                                 args_.table.inner)};
  TENZIR_ASSERT(not transformations_);
  transformations_.emplace();
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
      transformations_->transformations.try_emplace(std::string{name},
                                                    std::move(functions));
    }
  };
  query.OnData(cb);
  client_.Execute(query);
  if (failed) {
    return failure::promise();
  }
  transformations_->found_column.resize(
    transformations_->transformations.size(), false);
  return {};
}

auto easy_client::create_table(const tenzir::record_type& schema)
  -> failure_or<void> {
  TENZIR_ASSERT(args_.primary);
  auto columns = std::string{};
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
                  creation_modifier, args_.table.inner, clickhouse_columns,
                  engine, args_.primary->inner);
  auto query = Query{query_text};
  client_.Execute(query);
  TRY(get_schema_transformations());
  return {};
}

auto easy_client::insert(const table_slice& slice) -> bool {
  if (not transformations_) {
    TENZIR_DEBUG("creating table");
    const auto& schema = as<record_type>(slice.schema());
    if (not create_table(schema)) {
      return false;
    }
    TENZIR_DEBUG("created table");
    TENZIR_ASSERT(transformations_);
  }
  dropmask_.clear();
  dropmask_.resize(slice.rows());
  TENZIR_ASSERT(transformations_);
  auto updated = transformer::drop::none;
  path_type path{};
  /// TODO: This should really be merged with the transformer itself. Its an
  /// (almost) duplicate of `make_record_functions_from_clickhouse`
  for (const auto& [k, t, arr] : columns_of(slice)) {
    auto [trafo, idx] = transformations_->transfrom_and_index_for(k);
    if (not trafo) {
      diagnostic::warning("column `{}` does not exist in the ClickHouse table",
                          k)
        .note("column will be dropped")
        .primary(args_.operator_location)
        .emit(dh_);
      continue;
    }
    transformations_->found_column[idx] = true;
    path.push_back(k);
    updated = updated | trafo->update_dropmask(path, t, arr, dropmask_, dh_);
    path.pop_back();
    if (updated == transformer::drop::all) {
      // has already been reported
      return false;
    }
  }
  for (const auto& [i, kvp] :
       detail::enumerate(transformations_->transformations)) {
    if (transformations_->found_column[i]) {
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
    const auto [trafo, out_idx] = transformations_->transfrom_and_index_for(k);
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
    client_.Insert(args_.table.inner, block);
  }
  return true;
}

void easy_client::ping() {
  client_.Ping();
}
} // namespace tenzir::plugins::clickhouse

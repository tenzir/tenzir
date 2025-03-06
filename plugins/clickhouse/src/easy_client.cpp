//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause
#include "easy_client.hpp"

#include "clickhouse/client.h"
#include "tenzir/detail/enumerate.hpp"
#include "tenzir/view3.hpp"

#include <boost/regex.hpp>

#include <ranges>

using namespace clickhouse;
using namespace std::string_view_literals;

/// The implementation works via multiple customization points:
/// * Implementing the entire transformer by hand. This is done for Tuple and
///   Array
/// * A common implementation for non-structural types that works via
///   * A trait to handle names and allocations.
///     * This trait is auto-implemented for most types via a X-macro.
///     * It is specialized for some types with special requirements
///   * A `value_transform` function that translates between a tenzir::data
///     value and the expected clickhouse API value.

namespace tenzir::plugins::clickhouse {

auto Easy_Client::make(Arguments args,
                       diagnostic_handler& dh) -> std::unique_ptr<Easy_Client> {
  auto client = std::make_unique<Easy_Client>(std::move(args), dh);
  /// Note that technically, we have a ToCToU bug here. The table could be
  /// created or deleted in between this, the `get` call below and the potential
  /// creation in `insert`
  const auto table_existed = client->table_exists();
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

auto Easy_Client::table_exists() -> bool {
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

auto Easy_Client::get_schema_transformations() -> bool {
  auto query = Query{fmt::format("DESCRIBE TABLE {} "
                                 "SETTINGS describe_compact_output=1",
                                 args_.table.inner)};
  auto error = false;
  auto cb = [&](const Block& block) {
    for (size_t i = 0; i < block.GetRowCount(); ++i) {
      auto name = block[0]->As<ColumnString>()->At(i);
      auto type_str = remove_non_significant_whitespace(
        block[1]->As<ColumnString>()->At(i));
      auto functions = make_functions_from_clickhouse(type_str);
      if (not functions) {
        error = true;
        auto diag
          = diagnostic::error("unsupported column type in pre-existing table "
                              "`{}`",
                              args_.table.inner)
              .primary(args_.operator_location)
              .note("column `{}` has unsupported type `{}`", name, type_str);
        // A few helpful suggestions for the types that we do support
        if (name.starts_with("Date")) {
          diag = std::move(diag).note("use `DateTime64(8)` instead");
        } else if (name.starts_with("UInt")) {
          diag = std::move(diag).note("use `UInt64` instead");
        } else if (name.starts_with("Int")) {
          diag = std::move(diag).note("use `Int64` instead");
        } else if (name.starts_with("Float")) {
          diag = std::move(diag).note("use `Float64` instead");
        } else if (name == "IPv4") {
          diag = std::move(diag).note("use `IPv6` instead");
        }
        std::move(diag).emit(dh_);
      }
      transformations_->transformations.try_emplace(std::string{name},
                                                    std::move(functions));
    }
  };
  query.OnData(cb);
  client_.Execute(query);
  if (error) {
    return true;
  }
  transformations_->found_column.resize(
    transformations_->transformations.size(), false);
  return false;
}

auto Easy_Client::create_table(const tenzir::record_type& schema) -> bool {
  auto columns = std::string{};
  auto trafos = transformer_record::schema_transformations{};
  auto primary_found = false;
  for (auto [k, t] : schema.fields()) {
    auto clickhouse_type
      = type_to_clickhouse_typename(t, k != args_.primary->inner);
    auto functions = make_functions_from_clickhouse(clickhouse_type);
    if (not functions) {
      diagnostic::error("cannot create table: unsupported column type in input")
        .primary(args_.operator_location)
        .note("type `{}` is not supported", t)
        .emit(dh_);
      return false;
    }
    primary_found |= k == args_.primary->inner;
    trafos.try_emplace(std::string{k}, std::move(functions));
  }
  if (not primary_found) {
    diagnostic::error(
      "cannot create table: primary key does not exist in input")
      .primary(*args_.primary, "column `{}` does not exist",
               args_.primary->inner)
      .emit(dh_);
    return false;
  }
  transformations_ = transformer_record{"UNUSED", std::move(trafos)};
  constexpr static std::string_view engine = "MergeTree";
  auto query_text
    = fmt::format("CREATE TABLE {}"
                  " {}"
                  " ENGINE = {}"
                  " ORDER BY {}",
                  args_.table.inner,
                  plain_clickhouse_tuple_elements(schema, args_.primary->inner),
                  engine, args_.primary->inner);
  auto query = Query{query_text};
  client_.Execute(query);
  return true;
}

auto Easy_Client::insert(const table_slice& slice) -> bool {
  if (not transformations_) {
    auto& schema = as<record_type>(slice.schema());
    if (not create_table(schema)) {
      return false;
    }
    TENZIR_TRACE("created table");
  }
  dropmask_.clear();
  dropmask_.resize(slice.rows());
  TENZIR_ASSERT(transformations_);
  auto updated = transformer::drop::none;
  for (const auto& [k, t, arr] : columns_of(slice)) {
    auto [trafo, idx] = transformations_->transfrom_and_index_for(k);
    if (not trafo) {
      /// TODO diagnostic
    }
    transformations_->found_column[idx] = true;
    updated = updated | trafo->update_dropmask(t, arr, dropmask_, dh_);
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
  }
  if (updated == transformer::drop::all) {
    return false;
  }
  auto block = ::clickhouse::Block{};
  for (const auto& [k, t, arr] : columns_of(slice)) {
    const auto [trafo, out_idx] = transformations_->transfrom_and_index_for(k);
    if (not trafo) {
      continue;
    }
    auto this_column = trafo->create_column(t, arr, dropmask_, dh_);
    // TODO: re-evaluate this
    TENZIR_ASSERT(this_column);
    block.AppendColumn(std::string{k}, std::move(this_column));
  }

  if (block.GetColumnCount() > 0) {
    client_.Insert(args_.table.inner, block);
  }
  return true;
}
} // namespace tenzir::plugins::clickhouse

//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "clickhouse/table_registry.hpp"

#include "clickhouse/transformers.hpp"
#include "tenzir/async/blocking_executor.hpp"
#include "tenzir/detail/string.hpp"

#include <fmt/format.h>

#include <algorithm>

namespace tenzir::plugins::clickhouse {

namespace {

/// Finds `JSON` or `Tuple` anywhere in a type, which mapped columns cannot be.
auto unsupported_mapping_type(std::string_view type)
  -> Option<std::string_view> {
  for (auto kind : {"JSON", "Tuple"}) {
    if (unwrap_clickhouse_type_call(type, kind)) {
      return std::string_view{kind};
    }
  }
  for (auto wrapper : {"Array", "Nullable", "LowCardinality", "Map"}) {
    if (auto inner = unwrap_clickhouse_type_call(type, wrapper)) {
      for (auto argument : split_top_level_clickhouse_type_arguments(*inner)) {
        if (auto unsupported = unsupported_mapping_type(argument)) {
          return unsupported;
        }
      }
    }
  }
  return None{};
}

/// Returns the input path that fills the mapped column `column`.
auto mapping_path(ColumnDescription const& column, std::string_view type,
                  std::span<Mapping const> mappings, diagnostic_handler& dh)
  -> failure_or<std::vector<std::string>> {
  if (auto unsupported = unsupported_mapping_type(type)) {
    diagnostic::error("unsupported {} mapping for column `{}`", *unsupported,
                      column.name)
      .emit(dh);
    return failure::promise();
  }
  auto path = std::vector<std::string>{};
  for (auto component : detail::split(column.name, ".")) {
    if (component.empty()) {
      diagnostic::error("empty ClickHouse mapping path component").emit(dh);
      return failure::promise();
    }
    path.emplace_back(component);
  }
  for (auto const& other : mappings) {
    auto const count = std::min(path.size(), other.path.size());
    if (std::equal(path.begin(), path.begin() + count, other.path.begin())) {
      diagnostic::error("overlapping ClickHouse mapping paths `{}` and `{}`",
                        other.column, column.name)
        .emit(dh);
      return failure::promise();
    }
  }
  return path;
}

auto find_catch_all(std::vector<ColumnDescription> const& description,
                    diagnostic_handler& dh) -> failure_or<Option<std::string>> {
  auto result = Option<std::string>{};
  for (auto const& column : description) {
    if (column.comment != "tenzir:catch_all") {
      continue;
    }
    if (result) {
      diagnostic::error("multiple ClickHouse catch-all columns").emit(dh);
      return failure::promise();
    }
    if (column.name.find('.') != std::string::npos) {
      diagnostic::error("ClickHouse catch-all column name `{}` must not "
                        "contain dots",
                        column.name)
        .emit(dh);
      return failure::promise();
    }
    // Restricted JSON declarations can discard paths or coerce values.
    if (column.type != "JSON" or not column.default_kind.empty()) {
      diagnostic::error("invalid ClickHouse catch-all column `{}`", column.name)
        .note("expected a native JSON column without a default expression")
        .emit(dh);
      return failure::promise();
    }
    result = column.name;
  }
  return result;
}

auto same(Arc<TableSchema const> const& lhs, Arc<TableSchema const> const& rhs)
  -> bool {
  return &*lhs == &*rhs;
}

/// Whether the schema in `entry` must be fetched again.
auto expired(auto const& entry) -> bool {
  return entry.schema and (*entry.schema)->root.catch_all
         and std::chrono::steady_clock::now() - entry.refreshed
               >= schema_refresh_interval;
}

/// Stores `description` in `entry`. Returns whether the schema changed.
auto update(auto& entry, std::vector<ColumnDescription> description,
            diagnostic_handler& dh) -> failure_or<bool> {
  entry.refreshed = std::chrono::steady_clock::now();
  if (entry.schema and (*entry.schema)->description == description) {
    return false;
  }
  TRY(auto schema, build_table_schema(std::move(description), dh));
  if (entry.schema and (*entry.schema)->root.catch_all
      and not schema.root.catch_all) {
    diagnostic::error("ClickHouse catch-all marker was removed").emit(dh);
    return failure::promise();
  }
  entry.schema = Arc<TableSchema const>{std::move(schema)};
  return true;
}

} // namespace

auto build_table_schema(std::vector<ColumnDescription> description,
                        diagnostic_handler& dh) -> failure_or<TableSchema> {
  TRY(auto catch_all, find_catch_all(description, dh));
  auto root = RootWriter{};
  auto mappings = std::vector<Mapping>{};
  auto const mapping
    = catch_all ? ColumnMapping::lossless : ColumnMapping::legacy;
  for (auto const& column : description) {
    auto const type = remove_non_significant_whitespace(column.type);
    auto const has_default = not column.default_kind.empty();
    // EPHEMERAL columns are omitted in catch-all tables, so the input stays in
    // the catch-all.
    auto const generated
      = column.default_kind == "MATERIALIZED" or column.default_kind == "ALIAS"
        or (catch_all and column.default_kind == "EPHEMERAL");
    if (catch_all) {
      if (column.comment.find("tenzir:type=") != std::string::npos) {
        diagnostic::error(
          "unsupported ClickHouse type annotation on column `{}`", column.name)
          .note("type annotations are not supported in tables with a "
                "catch-all column")
          .emit(dh);
        return failure::promise();
      }
    }
    auto const is_catch_all = catch_all and column.name == *catch_all;
    if (catch_all and not generated and not is_catch_all) {
      TRY(auto path, mapping_path(column, type, mappings, dh));
      mappings.push_back({column.name, std::move(path)});
    }
    if (generated) {
      root.generated.insert(column.name);
      continue;
    }
    auto collector = collecting_diagnostic_handler{};
    auto writer = make_column_writer(column.name, type, mapping, collector);
    if (not writer and has_default) {
      writer = make_default_only_writer(column.name, type);
    } else {
      std::move(collector).forward_to(dh);
    }
    if (not writer) {
      return failure::promise();
    }
    root.columns.push_back({column.name, std::move(*writer), has_default});
  }
  if (catch_all) {
    root.catch_all = CatchAll{*catch_all, std::move(mappings)};
  }
  return TableSchema{std::move(description), std::move(root)};
}

auto TableRegistry::lookup(std::string_view table)
  -> Task<std::pair<SharedEntry, MutexGuard<Entry>>> {
  auto tables = co_await tables_.lock();
  if (auto it = tables->find(table); it != tables->end()) {
    auto entry = it->second;
    tables.unlock();
    auto guard = co_await entry->lock();
    co_return std::pair{std::move(entry), std::move(guard)};
  }
  // Lock the new entry before others can see it, so that they wait for the
  // first fetch.
  auto entry = SharedEntry{std::in_place};
  auto guard = co_await entry->lock();
  tables->emplace(std::string{table}, entry);
  tables.unlock();
  co_return std::pair{std::move(entry), std::move(guard)};
}

auto TableRegistry::fetch(std::string_view table, Connection& connection,
                          std::span<nova::Events const> events,
                          diagnostic_handler& dh)
  -> failure_or<std::vector<ColumnDescription>> {
  auto split = split_table_name<true>(table, options_.table, dh);
  if (not split) {
    return failure::promise();
  }
  // Check all preconditions before changing anything. The table may still be
  // created or dropped concurrently by someone else.
  TRY(auto const table_existed, exists(connection, "TABLE", table, dh));
  auto const& mode = options_.mode;
  if (mode.inner == mode::create and table_existed) {
    diagnostic::error("mode is `create`, but table `{}` already exists", table)
      .primary(mode)
      .primary(options_.table)
      .emit(dh);
    return failure::promise();
  }
  if (mode.inner == mode::create_append and not table_existed
      and not options_.primary) {
    diagnostic::error("table `{}` does not exist, but no `primary` was "
                      "specified",
                      table)
      .primary(options_.table)
      .emit(dh);
    return failure::promise();
  }
  if (mode.inner == mode::append and not table_existed) {
    diagnostic::error("mode is `append`, but table `{}` does not exist", table)
      .primary(mode)
      .primary(options_.table)
      .emit(dh);
    return failure::promise();
  }
  if (split->database) {
    TRY(auto const database_existed,
        exists(connection, "DATABASE", *split->database, dh));
    if (not database_existed) {
      if (mode.inner == mode::append) {
        diagnostic::error("mode is `append`, but database `{}` does not exist",
                          *split->database)
          .primary(mode)
          .primary(options_.table)
          .emit(dh);
        return failure::promise();
      }
      create_database(connection, *split->database);
    }
  }
  if (not table_existed) {
    TRY(auto query, make_create_table_query(table, events, options_, dh));
    create_table(connection, std::move(query));
  }
  return describe_table(connection, table, dh);
}

auto TableRegistry::get(std::string_view table, Connection& connection,
                        std::span<nova::Events const> events,
                        diagnostic_handler& dh)
  -> Task<failure_or<Arc<TableSchema const>>> {
  auto [entry, guard] = co_await lookup(table);
  if (guard->schema and not expired(*guard)) {
    co_return *guard->schema;
  }
  auto description = co_await spawn_blocking([&] {
    return guard->schema ? describe_table(connection, table, dh)
                         : fetch(table, connection, events, dh);
  });
  if (not description) {
    co_return failure::promise();
  }
  if (not update(*guard, std::move(*description), dh)) {
    co_return failure::promise();
  }
  if ((*guard->schema)->root.catch_all) {
    has_catch_all_.store(true, std::memory_order_relaxed);
  }
  co_return *guard->schema;
}

auto TableRegistry::invalidate(std::string_view table,
                               Arc<TableSchema const> const& rejected,
                               Connection& connection, diagnostic_handler& dh)
  -> Task<failure_or<Option<Arc<TableSchema const>>>> {
  auto [entry, guard] = co_await lookup(table);
  if (guard->schema and not same(*guard->schema, rejected)) {
    co_return *guard->schema;
  }
  auto description = co_await spawn_blocking([&] {
    return describe_table(connection, table, dh);
  });
  if (not description) {
    co_return failure::promise();
  }
  auto changed = update(*guard, std::move(*description), dh);
  if (not changed) {
    co_return failure::promise();
  }
  if (not *changed) {
    co_return None{};
  }
  co_return *guard->schema;
}

auto TableRegistry::refresh_expired(Connection& connection,
                                    diagnostic_handler& dh)
  -> Task<failure_or<Refresh>> {
  auto entries = std::vector<std::pair<std::string, SharedEntry>>{};
  {
    auto tables = co_await tables_.lock();
    for (auto const& [table, entry] : *tables) {
      entries.emplace_back(table, entry);
    }
  }
  auto result = Refresh{};
  for (auto& [table, entry] : entries) {
    auto guard = co_await entry->lock();
    if (expired(*guard)) {
      result.contacted = true;
      auto description = co_await spawn_blocking([&] {
        return describe_table(connection, table, dh);
      });
      if (not description) {
        co_return failure::promise();
      }
      if (not update(*guard, std::move(*description), dh)) {
        co_return failure::promise();
      }
    }
    if (guard->schema and (*guard->schema)->root.catch_all) {
      auto const due = guard->refreshed + schema_refresh_interval;
      result.next = result.next ? std::min(*result.next, due) : due;
    }
  }
  co_return result;
}

} // namespace tenzir::plugins::clickhouse

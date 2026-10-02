//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "clickhouse/block_builder.hpp"
#include "clickhouse/connection.hpp"
#include "clickhouse/create_table.hpp"
#include "tenzir/arc.hpp"
#include "tenzir/async/mutex.hpp"
#include "tenzir/async/task.hpp"
#include "tenzir/atomic.hpp"
#include "tenzir/detail/heterogeneous_string_hash.hpp"

#include <chrono>
#include <span>
#include <string_view>
#include <utility>
#include <vector>

namespace tenzir::plugins::clickhouse {

/// What we know about one destination table, built from one `DESCRIBE`.
/// Immutable and shared by every connection.
struct TableSchema {
  std::vector<ColumnDescription> description;
  RootWriter root;
};

/// Builds the writers for the columns in `description`.
auto build_table_schema(std::vector<ColumnDescription> description,
                        diagnostic_handler& dh) -> failure_or<TableSchema>;

/// The outcome of `TableRegistry::refresh_expired`.
struct Refresh {
  /// Whether a schema was fetched from the server.
  bool contacted = false;
  /// When the next schema of a catch-all table expires, if any.
  Option<std::chrono::steady_clock::time_point> next = None{};
};

/// What all workers know about the destination tables. A table that one worker
/// fetched or updated is visible to every other worker.
class TableRegistry {
public:
  explicit TableRegistry(TableOptions options) : options_{std::move(options)} {
  }

  /// Returns the schema of the qualified `table`. Fetches an unknown table
  /// from the server, after creating it according to the mode, with column
  /// types inferred from `events`. Fetches the schema of a catch-all table
  /// again once it expired.
  auto get(std::string_view table, Connection& connection,
           std::span<nova::Events const> events, diagnostic_handler& dh)
    -> Task<failure_or<Arc<TableSchema const>>>;

  /// Reports that the server rejected an insert built with `rejected`.
  /// Returns the newer schema if another worker already replaced `rejected`.
  /// Otherwise fetches the schema again and returns it if it changed.
  auto
  invalidate(std::string_view table, Arc<TableSchema const> const& rejected,
             Connection& connection, diagnostic_handler& dh)
    -> Task<failure_or<Option<Arc<TableSchema const>>>>;

  /// Fetches the schemas of catch-all tables again once they expired, so that
  /// idle writers notice changes, too.
  auto refresh_expired(Connection& connection, diagnostic_handler& dh)
    -> Task<failure_or<Refresh>>;

  /// Whether a catch-all table is known, which `refresh_expired` refreshes.
  auto has_catch_all() const -> bool {
    return has_catch_all_.load(std::memory_order_relaxed);
  }

private:
  struct Entry {
    /// Empty only while the first fetch runs, or after it failed.
    Option<Arc<TableSchema const>> schema = None{};
    std::chrono::steady_clock::time_point refreshed = {};
  };

  /// Holding the lock of an entry means that an update of the table is in
  /// progress.
  using SharedEntry = Arc<Mutex<Entry>>;

  /// Returns the locked entry of `table`, creating it if necessary.
  auto lookup(std::string_view table)
    -> Task<std::pair<SharedEntry, MutexGuard<Entry>>>;

  /// Creates the table if necessary and returns its description.
  auto fetch(std::string_view table, Connection& connection,
             std::span<nova::Events const> events, diagnostic_handler& dh)
    -> failure_or<std::vector<ColumnDescription>>;

  TableOptions options_;
  /// The lock is only held to look up an entry, never across remote calls.
  Mutex<detail::heterogeneous_string_hashmap<SharedEntry>> tables_;
  Atomic<bool> has_catch_all_{false};
};

} // namespace tenzir::plugins::clickhouse

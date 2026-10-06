//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "clickhouse/column_writer.hpp"
#include "tenzir/detail/heterogeneous_string_hash.hpp"
#include "tenzir/nova/events.hpp"

#include <clickhouse/block.h>

#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace tenzir::plugins::clickhouse {

/// A field of a batch: its values, and the rows that have it.
using Field = nova::MaskedArray<nova::Array<nova::Data>>;

/// A top-level column of the destination table. It is filled from the input
/// field of the same name.
struct RootColumn {
  std::string name;
  Box<ColumnWriter> writer;
  /// Whether the server fills the column when an insert omits it.
  bool has_default = false;
};

/// A column of a catch-all table and the input path that fills it.
struct Mapping {
  std::string column;
  std::vector<std::string> path;
};

/// The catch-all column of a table, which receives everything that no other
/// column takes.
struct CatchAll {
  std::string column;
  std::vector<Mapping> mappings;
};

/// Writes events into the columns of one table.
struct RootWriter {
  std::vector<RootColumn> columns;
  /// Columns that the server computes, so they cannot be written.
  detail::heterogeneous_string_hashset generated;
  Option<CatchAll> catch_all = None{};
};

/// One INSERT.
struct PreparedInsert {
  ::clickhouse::Block block;
  /// The number of rows in `block`.
  uint64_t rows = 0;
  /// The input rows of each batch that this insert covers, including rows that
  /// were rejected.
  std::vector<nova::storage::BitMap> input_rows;
};

/// Turns the active rows of `events` into as few inserts as possible. Rows
/// that omit different columns with defaults end up in different inserts,
/// because the server only fills defaults for columns that an insert omits. A
/// `null` for a column with a default omits the column as well, unless it is
/// `Nullable`.
auto build_inserts(RootWriter const& root, std::span<nova::Events const> events,
                   std::string_view table, diagnostic_handler& dh)
  -> std::vector<PreparedInsert>;

/// Reshapes `records` for a catch-all table. Every mapped path becomes a
/// top-level field named after its column, and all other fields move into the
/// catch-all field. Records that the mapping empties are removed, unless they
/// were empty to begin with.
auto reshape_for_catch_all(nova::Array<nova::Record> const& records,
                           CatchAll const& catch_all)
  -> nova::Array<nova::Record>;

/// Selects the field at `path` from `records`. Rows without the field are
/// absent.
auto select_path(nova::Array<nova::Record> const& records,
                 std::span<std::string const> path) -> Field;

} // namespace tenzir::plugins::clickhouse

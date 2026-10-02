//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "clickhouse/arguments.hpp"
#include "tenzir/nova/events.hpp"

#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace tenzir::plugins::clickhouse {

/// How tables are found and created.
struct TableOptions {
  located<enum mode> mode = {mode::create_append, location::unknown};
  Option<located<std::string>> primary = None{};
  /// Fields that become `JSON` columns when creating a table.
  std::vector<located<field_path_type>> json;
  /// Fields that become `LowCardinality` columns when creating a table.
  std::vector<located<field_path_type>> low_cardinality;
  /// The location of the `table` argument.
  location table = location::unknown;
  /// Raw, unquoted database name. Table names are qualified with it.
  Option<std::string> default_database = None{};
};

/// Builds the `CREATE TABLE` statement for `table`, inferring the column types
/// from the active rows of `events`.
auto make_create_table_query(std::string_view table,
                             std::span<nova::Events const> events,
                             TableOptions const& options,
                             diagnostic_handler& dh) -> failure_or<std::string>;

} // namespace tenzir::plugins::clickhouse

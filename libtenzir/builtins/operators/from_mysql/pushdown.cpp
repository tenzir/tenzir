//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/builtins/from_mysql/pushdown.hpp"

#include <tenzir/detail/string.hpp>

#include <fmt/chrono.h>
#include <fmt/format.h>
#include <fmt/ranges.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <utility>

namespace tenzir::plugins::mysql {

namespace {

auto has_nul(std::string_view text) -> bool {
  return text.find('\0') != std::string_view::npos;
}

} // namespace

auto quote_identifier(std::string_view name) -> std::string {
  auto result = std::string{"`"};
  for (auto c : name) {
    if (c == '`') {
      result += "``";
    } else {
      result += c;
    }
  }
  result += '`';
  return result;
}

auto parse_table_columns(std::span<std::vector<Option<std::string>> const> rows)
  -> std::vector<TableColumn> {
  auto result = std::vector<TableColumn>{};
  for (auto const& row : rows) {
    if (row.size() != 2 or not row[0]) {
      continue;
    }
    auto extra = row[1] ? detail::ascii_tolower(*row[1]) : std::string{};
    result.push_back(TableColumn{
      .name = *row[0],
      .visible = extra.find("invisible") == std::string::npos,
    });
  }
  return result;
}

auto column_names(std::span<TableColumn const> columns)
  -> std::vector<std::string> {
  auto result = std::vector<std::string>{};
  result.reserve(columns.size());
  for (auto const& column : columns) {
    result.push_back(column.name);
  }
  return result;
}

auto select_columns(Option<ir::OptimizeProjection> const& projection,
                    std::span<TableColumn const> columns,
                    std::span<std::string const> required,
                    std::span<ir::QueryStage const> stages,
                    std::string_view table) -> Selection {
  // A projection keeps what `SELECT *` yields, so only visible columns take
  // part in it.
  auto visible = std::vector<std::string>{};
  for (auto const& column : columns) {
    if (column.visible) {
      visible.push_back(column.name);
    }
  }
  auto result = Selection{
    .selection = pushdown::select_projection(projection, visible, required,
                                             stages, MysqlDialect{}),
    .extra = {},
  };
  // The stages may read invisible columns, which the selection adds for them
  // and the result leaves out again.
  auto const inputs = pushdown::plan_inputs(stages);
  auto invisible = std::vector<std::string>{};
  for (auto const& column : columns) {
    if (column.visible) {
      continue;
    }
    // MySQL resolves column names without regard to case.
    auto const read
      = not inputs or std::ranges::any_of(*inputs, [&](auto const& name) {
          return detail::ascii_icase_equal(name, column.name);
        });
    if (read) {
      invisible.push_back(mysql::quote_identifier(column.name));
      result.extra.push_back(column.name);
    }
  }
  if (invisible.empty()) {
    return result;
  }
  result.selection
    = result.selection == "*"
        ? fmt::format("{}.*, {}", table, fmt::join(invisible, ", "))
        : fmt::format("{}, {}", result.selection, fmt::join(invisible, ", "));
  return result;
}

auto MysqlDialect::quote_identifier(std::string_view name) const
  -> std::string {
  return mysql::quote_identifier(name);
}

auto MysqlDialect::quote_string(std::string_view text) const
  -> Option<std::string> {
  // Whether MySQL reads a backslash as an escape depends on the session's
  // `NO_BACKSLASH_ESCAPES` mode, and double quotes delimit identifiers in
  // `ANSI_QUOTES` mode, so the literal uses single quotes only.
  if (text.find_first_of(std::string_view{"\\\0", 2})
      != std::string_view::npos) {
    return None{};
  }
  return SqlQueryDialect::quote_string(text);
}

auto MysqlDialect::bind_field(std::span<std::string const> path) const
  -> Option<std::string> {
  // MySQL has no records in columns.
  if (path.size() != 1 or has_nul(path.front())) {
    return None{};
  }
  return mysql::quote_identifier(path.front());
}

auto MysqlDialect::render_time(time value) const -> Option<std::string> {
  // MySQL keeps microseconds.
  auto const micros
    = std::chrono::time_point_cast<std::chrono::microseconds>(value);
  if (micros != value) {
    return None{};
  }
  auto const seconds = std::chrono::floor<std::chrono::seconds>(micros);
  return fmt::format("'{:%Y-%m-%d %H:%M:%S}.{:06}'", seconds,
                     (micros - seconds).count());
}

auto MysqlDialect::render_ip(ip value) const -> Option<std::string> {
  return SqlQueryDialect::quote_string(fmt::to_string(value));
}

/// `LIKE` would need escaping of a pattern that is no literal, and
/// `REGEXP_LIKE` matches with the case sensitivity of RE2. The time functions
/// read `DATETIME` and `DATE` values as UTC, as comparisons do.
auto MysqlDialect::functions() const -> std::span<pushdown::Function const> {
  static constexpr auto rows = std::array{
    pushdown::Function{"starts_with", 2, "(LEFT({0}, CHAR_LENGTH({1})) = {1})",
                       pushdown::Decode::native, true},
    pushdown::Function{"ends_with", 2, "(RIGHT({0}, CHAR_LENGTH({1})) = {1})",
                       pushdown::Decode::native, true},
    pushdown::Function{"match_regex", 2, "REGEXP_LIKE({0}, {1}, 'c')",
                       pushdown::Decode::native, true},
    pushdown::Function{"length_bytes", 1, "LENGTH({0})"},
    pushdown::Function{"length_chars", 1, "CHAR_LENGTH({0})"},
    pushdown::Function{"to_lower", 1, "LOWER({0})"},
    pushdown::Function{"to_upper", 1, "UPPER({0})"},
    pushdown::Function{"capitalize", 1,
                       "CONCAT(UPPER(LEFT({0}, 1)), LOWER(SUBSTRING({0}, 2)))"},
    pushdown::Function{"trim", 1, "TRIM({0})"},
    pushdown::Function{"trim_start", 1, "LTRIM({0})"},
    pushdown::Function{"trim_end", 1, "RTRIM({0})"},
    pushdown::Function{"replace", 3, "REPLACE({0}, {1}, {2})"},
    pushdown::Function{"is_empty", 1, "(CHAR_LENGTH({0}) = 0)",
                       pushdown::Decode::native, true},
    // Negative bounds of `slice` stay in the pipeline.
    pushdown::Function{"slice", 1, "SUBSTRING({0}, {begin:nat} + 1)"},
    pushdown::Function{"slice", 1, "LEFT({0}, {end:nat})"},
    pushdown::Function{"slice", 1,
                       "SUBSTRING({0}, {begin:nat} + 1, GREATEST({end:nat} - "
                       "{begin:nat}, 0))"},
    pushdown::Function{"hash_md5", 1, "MD5({0})"},
    pushdown::Function{"hash_sha1", 1, "SHA1({0})"},
    pushdown::Function{"hash_sha256", 1, "SHA2({0}, 256)"},
    pushdown::Function{"abs", 1, "ABS({0})"},
    pushdown::Function{"ceil", 1, "CAST(CEIL({0}) AS SIGNED)"},
    pushdown::Function{"floor", 1, "CAST(FLOOR({0}) AS SIGNED)"},
    pushdown::Function{"round", 1, "CAST(ROUND({0}) AS SIGNED)"},
    pushdown::Function{"sqrt", 1, "SQRT({0})"},
    pushdown::Function{"exp", 1, "EXP({0})"},
    pushdown::Function{"log", 1, "LN({0})"},
    pushdown::Function{"log", 2, "LOG({1}, {0})"},
    pushdown::Function{"year", 1, "YEAR({0})"},
    pushdown::Function{"month", 1, "MONTH({0})"},
    pushdown::Function{"day", 1, "DAY({0})"},
    pushdown::Function{"hour", 1, "HOUR({0})"},
    pushdown::Function{"minute", 1, "MINUTE({0})"},
    pushdown::Function{"second", 1, "(SECOND({0}) + MICROSECOND({0}) / 1e6)"},
    // TQL buckets times by multiples of the width since the epoch, also
    // before it, which flooring the microseconds since then reproduces. The
    // result arrives as text.
    pushdown::Function{"floor", 2,
                       "TIMESTAMPADD(MICROSECOND, "
                       "TIMESTAMPDIFF(MICROSECOND, '1970-01-01', {0}) - "
                       "MOD(TIMESTAMPDIFF(MICROSECOND, '1970-01-01', {0}), "
                       "{1:us}) - CASE WHEN "
                       "MOD(TIMESTAMPDIFF(MICROSECOND, '1970-01-01', {0}), "
                       "{1:us}) < 0 THEN {1:us} ELSE 0 END, '1970-01-01')",
                       pushdown::Decode::time},
  };
  static_assert(pushdown::valid_rows(rows));
  return rows;
}

auto MysqlDialect::aggregations() const
  -> std::span<pushdown::Aggregation const> {
  static auto const rows = [this] {
    auto const base = SqlQueryDialect::aggregations();
    auto result = std::vector<pushdown::Aggregation>(base.begin(), base.end());
    result.push_back({"all", 1, "MIN({0} <> 0)", pushdown::Decode::boolean});
    result.push_back({"any", 1, "MAX({0} <> 0)", pushdown::Decode::boolean});
    return result;
  }();
  return rows;
}

auto MysqlDialect::time_literal_arithmetic() const -> bool {
  return false;
}

/// MySQL has no booleans: a predicate as a value is encoded as 0/1 and decoded
/// back as a boolean.
auto MysqlDialect::boolean_values() const -> bool {
  return false;
}

auto MysqlDialect::render_predicate_value(std::string_view predicate) const
  -> Option<std::string> {
  return fmt::format("CASE WHEN {} THEN 1 ELSE 0 END", predicate);
}

auto planner(pushdown::SqlMode mode)
  -> pushdown::Planner<pushdown::TableRead, pushdown::SqlTable> {
  static auto const dialect = MysqlDialect{};
  return pushdown::sql_planner("MySQL", dialect, mode);
}

} // namespace tenzir::plugins::mysql

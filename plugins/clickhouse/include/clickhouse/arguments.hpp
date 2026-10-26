//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "clickhouse/client.h"
#include "tenzir/argument_parser2.hpp"
#include "tenzir/detail/enum.hpp"
#include "tenzir/detail/string.hpp"
#include "tenzir/tls_options.hpp"
#include "tenzir/tql2/eval.hpp"
#include "tenzir/tql2/plugin.hpp"

#include <boost/regex.hpp>
#include <boost/url/parse.hpp>
#include <boost/url/url.hpp>

#include <algorithm>
#include <cctype>
#include <charconv>
#include <limits>
#include <span>
#include <vector>

namespace tenzir::plugins::clickhouse {

TENZIR_ENUM(mode, create_append, create, append);

constexpr static auto validation_expr = "^[a-zA-Z_][0-9a-zA-Z_]*$";
inline const auto table_name_quoting = detail::quoting_escaping_policy{
  .quotes = "\"`",
  .doubled_quotes_escape = true,
};

inline auto validate_identifier(std::string_view text) -> bool {
  if (table_name_quoting.is_quoted(text)) {
    return true;
  }
  const static auto re = boost::regex{validation_expr};
  return boost::regex_match(text.begin(), text.end(), re);
}

template <bool error>
inline auto diag_root(std::string_view msg) -> diagnostic_builder {
  if constexpr (error) {
    return diagnostic::error("{}", msg);
  }
  return diagnostic::warning("{}", msg);
}

template <bool error>
inline auto
emit_invalid_identifier(std::string_view name, std::string_view value,
                        location loc, diagnostic_handler& dh) {
  diag_root<error>(fmt::format("invalid {} `{}`", name, value))
    .primary(loc)
    .hint("{} must either be a quoted string, or match the regular "
          "expression `{}`",
          name, validation_expr)
    .emit(dh);
}

struct split_table_name_result {
  Option<std::string_view> database = None{};
  std::string_view table;
};

inline auto
parse_connection_uri(std::string_view uri, location loc, diagnostic_handler& dh)
  -> failure_or<boost::urls::url> {
  auto parsed = boost::urls::parse_uri(uri);
  if (not parsed) {
    diagnostic::error("failed to parse ClickHouse URI")
      .primary(loc)
      .note("{}", parsed.error().message())
      .hint("expected `clickhouse://[user[:password]@]host[:port][/database]`")
      .emit(dh);
    return failure::promise();
  }
  if (parsed->scheme() != "clickhouse") {
    diagnostic::error("invalid ClickHouse URI scheme `{}`", parsed->scheme())
      .primary(loc)
      .hint("expected `clickhouse://[user[:password]@]host[:port][/database]`")
      .emit(dh);
    return failure::promise();
  }
  if (parsed->host().empty()) {
    diagnostic::error("ClickHouse URI requires a host")
      .primary(loc)
      .hint("expected `clickhouse://[user[:password]@]host[:port][/database]`")
      .emit(dh);
    return failure::promise();
  }
  if (parsed->has_query() or parsed->has_fragment()) {
    diagnostic::error("ClickHouse URI does not support query parameters or "
                      "fragments")
      .primary(loc)
      .hint("expected `clickhouse://[user[:password]@]host[:port][/database]`")
      .emit(dh);
    return failure::promise();
  }
  auto segments = parsed->segments();
  if (segments.size() > 1) {
    diagnostic::error("ClickHouse URI path may contain at most one database "
                      "name")
      .primary(loc)
      .hint("expected `clickhouse://[user[:password]@]host[:port][/database]`")
      .emit(dh);
    return failure::promise();
  }
  return boost::urls::url{*parsed};
}

inline auto unquote_identifier_component(std::string_view text) -> std::string {
  return table_name_quoting.unquote_unescape(text);
}

inline auto quote_identifier_component(std::string_view text) -> std::string {
  return fmt::format("\"{}\"", detail::double_escape(text, "\""));
}

inline const auto clickhouse_type_quoting = detail::quoting_escaping_policy{
  .quotes = R"('"`)",
  .doubled_quotes_escape = true,
};

inline auto skip_quoted_token(std::string_view text, size_t& i) -> bool {
  if (not clickhouse_type_quoting.is_quote_character(text[i])) {
    return false;
  }
  if (auto closing = clickhouse_type_quoting.find_closing_quote(text, i);
      closing != std::string_view::npos) {
    i = closing;
  } else {
    i = text.size() - 1;
  }
  return true;
}

inline auto split_top_level_clickhouse_type_arguments(std::string_view text)
  -> std::vector<std::string_view> {
  auto result = std::vector<std::string_view>{};
  auto depth = size_t{0};
  auto begin = size_t{0};
  for (auto i = size_t{0}; i < text.size(); ++i) {
    if (skip_quoted_token(text, i)) {
      continue;
    }
    auto c = text[i];
    if (c == '(') {
      ++depth;
      continue;
    }
    if (c == ')') {
      TENZIR_ASSERT(depth > 0);
      --depth;
      continue;
    }
    if (c == ',' and depth == 0) {
      result.push_back(detail::trim(text.substr(begin, i - begin)));
      begin = i + 1;
    }
  }
  result.push_back(detail::trim(text.substr(begin)));
  return result;
}

inline auto find_top_level_clickhouse_type_space(std::string_view text)
  -> size_t {
  auto depth = size_t{0};
  for (auto i = size_t{0}; i < text.size(); ++i) {
    if (skip_quoted_token(text, i)) {
      continue;
    }
    auto c = text[i];
    if (c == '(') {
      ++depth;
      continue;
    }
    if (c == ')') {
      TENZIR_ASSERT(depth > 0);
      --depth;
      continue;
    }
    if (depth == 0 and std::isspace(static_cast<unsigned char>(c))) {
      return i;
    }
  }
  return std::string_view::npos;
}

inline auto
unwrap_clickhouse_type_call(std::string_view text, std::string_view name)
  -> Option<std::string_view> {
  if (not text.starts_with(name)) {
    return None{};
  }
  if (text.size() <= name.size() + 1 or text[name.size()] != '('
      or text.back() != ')') {
    return None{};
  }
  return text.substr(name.size() + 1, text.size() - name.size() - 2);
}

/// Whether `type` is `JSON` without typed or skipped paths, so its only
/// arguments, if any, are the storage settings `max_dynamic_paths` and
/// `max_dynamic_types`. These change how ClickHouse lays out paths, but never
/// discard paths or coerce values. Expects a type without non-significant
/// whitespace.
inline auto is_lossless_json(std::string_view type) -> bool {
  if (type == "JSON") {
    return true;
  }
  auto arguments = unwrap_clickhouse_type_call(type, "JSON");
  if (not arguments) {
    return false;
  }
  return std::ranges::all_of(
    split_top_level_clickhouse_type_arguments(*arguments),
    [](std::string_view argument) {
      return argument.starts_with("max_dynamic_paths=")
             or argument.starts_with("max_dynamic_types=");
    });
}

/// A top-level or nested field path, as a sequence of identifier segments
/// (e.g. `["file", "xattributes"]` for `file.xattributes`). Kept as segments
/// rather than a joined dotted string so it can be compared directly,
/// segment-by-segment, against the `path_type` accumulated while recursively
/// walking a schema (see `transformers.hpp`'s `path_type`).
using field_path_type = std::vector<std::string>;

/// Renders a field path as a dotted string for diagnostics.
inline auto to_dotted_string(std::span<const std::string> path) -> std::string {
  return fmt::format("{}", fmt::join(path, "."));
}

/// Returns the index of the first entry in `columns` whose segments equal
/// `path`, or `None` if absent. Used to look up a field path in the `json=`
/// and `low_cardinality=` argument lists.
inline auto index_of(std::span<const located<field_path_type>> columns,
                     std::span<const std::string_view> path) -> Option<size_t> {
  for (size_t i = 0; i < columns.size(); ++i) {
    if (std::ranges::equal(columns[i].inner, path)) {
      return i;
    }
  }
  return None{};
}

/// The clickhouse-cpp client only supports `LowCardinality` over `String` (and
/// its nullable form); it rejects or mishandles fixed-size inner types such as
/// numerics. Both the create-table path (`remote_create_table`) and the
/// append path (`make_functions_from_clickhouse`) gate on this same invariant.
inline auto is_lowcardinality_supported_inner(std::string_view inner) -> bool {
  return inner == "String" or inner == "Nullable(String)";
}

inline auto parse_clickhouse_size(std::string_view text, size_t& result)
  -> bool {
  auto value = uint64_t{0};
  auto [ptr, ec]
    = std::from_chars(text.data(), text.data() + text.size(), value);
  if (ec != std::errc{} or ptr != text.data() + text.size()
      or value > std::numeric_limits<size_t>::max()) {
    return false;
  }
  result = static_cast<size_t>(value);
  return true;
}

template <bool error>
inline auto split_table_name(std::string_view table, location table_loc,
                             diagnostic_handler& dh)
  -> Option<split_table_name_result> {
  if (auto split = table_name_quoting.split_at_unquoted(table, '.')) {
    if (split->second.empty()) {
      diag_root<error>("expected table name after `.`")
        .primary(table_loc)
        .emit(dh);
      return None{};
    }
    if (table_name_quoting.split_at_unquoted(split->second, '.')) {
      diag_root<error>("`table` may contain at most one `.`")
        .note("the `.` separates database and table name")
        .hint("quote the identifiers if you want the `.` to be part of the "
              "identifier")
        .primary(table_loc)
        .emit(dh);
      return None{};
    }
    return split_table_name_result{split->first, split->second};
  }
  return split_table_name_result{None{}, table};
}

template <bool error>
inline auto validate_table_name(std::string_view table, location table_loc,
                                diagnostic_handler& dh) -> bool {
  auto split = split_table_name<error>(table, table_loc, dh);
  if (not split) {
    return false;
  }
  if (split->database) {
    if (not validate_identifier(*split->database)) {
      emit_invalid_identifier<error>("database-part", *split->database,
                                     table_loc, dh);
      return false;
    }
    if (not validate_identifier(split->table)) {
      emit_invalid_identifier<error>("table-part", split->table, table_loc, dh);
      return false;
    }
  } else if (not validate_identifier(split->table)) {
    emit_invalid_identifier<error>("table", table, table_loc, dh);
    return false;
  }
  return true;
}

// `json=` and `low_cardinality=` each accept either a single field selector or
// a list of field selectors, where each selector may be a top-level or nested
// (dotted) field path. Extracts the validated, de-duplicated set of field
// paths. `argument_name` selects the argument named in diagnostics. The
// caller must ensure the expression was actually provided (i.e. `expr.kind`
// is set).
inline auto
parse_field_list_argument(const ast::expression& expr, diagnostic_handler& dh,
                          std::string_view argument_name = "json")
  -> failure_or<std::vector<located<field_path_type>>> {
  auto result = std::vector<located<field_path_type>>{};
  auto add = [&](const ast::expression& e) -> failure_or<void> {
    auto sel = ast::field_path::try_from(e);
    if (not sel or sel->has_this()) {
      diagnostic::error("`{}` expects a field", argument_name)
        .primary(e.get_location())
        .emit(dh);
      return failure::promise();
    }
    auto path = field_path_type{};
    path.reserve(sel->path().size());
    for (const auto& segment : sel->path()) {
      if (segment.has_question_mark) {
        diagnostic::error("`{}` does not support optional field access",
                          argument_name)
          .primary(segment.id.get_location())
          .emit(dh);
        return failure::promise();
      }
      const auto name = std::string{segment.id.name};
      if (not validate_identifier(name)) {
        emit_invalid_identifier<true>(argument_name, name,
                                      segment.id.get_location(), dh);
        return failure::promise();
      }
      path.push_back(name);
    }
    for (const auto& existing : result) {
      if (existing.inner == path) {
        // De-duplicate silently.
        return {};
      }
    }
    result.push_back({std::move(path), sel->get_location()});
    return {};
  };
  if (const auto* list = try_as<ast::list>(expr)) {
    for (const auto& item : list->items) {
      const auto* item_expr = try_as<ast::expression>(item);
      if (not item_expr) {
        diagnostic::error("`{}` expects a field", argument_name)
          .primary(into_location(item))
          .emit(dh);
        return failure::promise();
      }
      TRY(add(*item_expr));
    }
  } else {
    TRY(add(expr));
  }
  return result;
}

// Backwards-compatible alias for the `json=` argument.
inline auto
parse_json_field_argument(const ast::expression& expr, diagnostic_handler& dh)
  -> failure_or<std::vector<located<field_path_type>>> {
  return parse_field_list_argument(expr, dh, "json");
}

} // namespace tenzir::plugins::clickhouse

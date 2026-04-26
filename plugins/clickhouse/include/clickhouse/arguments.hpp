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

#include <cctype>
#include <charconv>
#include <limits>
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

struct operator_arguments {
  tenzir::location operator_location;
  Option<located<secret>> uri = None{};
  located<secret> host = {secret::make_literal("localhost"), operator_location};
  Option<located<uint64_t>> port = None{};
  located<secret> user = {secret::make_literal("default"), operator_location};
  located<secret> password = {secret::make_literal(""), operator_location};
  ast::expression table = {};
  located<enum mode> mode = located{mode::create_append, operator_location};
  Option<located<std::string>> primary = None{};
  tls_options ssl = {};

  static auto try_parse(std::string operator_name,
                        operator_factory_invocation inv, session ctx)
    -> failure_or<operator_arguments> {
    auto res = operator_arguments{inv.self.get_location()};
    auto mode_str = located<std::string>{
      to_string(mode::create_append),
      res.operator_location,
    };
    auto uri = Option<located<secret>>{};
    auto host = Option<located<secret>>{};
    auto port = Option<located<int64_t>>{};
    auto user = Option<located<secret>>{};
    auto password = Option<located<secret>>{};
    auto primary_selector = Option<ast::field_path>{};
    auto parser = argument_parser2::operator_(operator_name);
    parser.named("uri", uri);
    parser.named("host", host);
    parser.named("port", port);
    parser.named("user", user);
    parser.named("password", password);
    parser.named("table", res.table, "string");
    parser.named_optional("mode", mode_str);
    parser.named("primary", primary_selector, "field");
    res.ssl.add_tls_options(parser);
    TRY(parser.parse(inv, ctx));
    if (uri and (host or port or user or password)) {
      diagnostic::error(
        "`uri` and explicit connection arguments are mutually exclusive")
        .primary(uri->source)
        .emit(ctx);
      return failure::promise();
    }
    if (uri) {
      res.uri = std::move(uri);
    }
    if (host) {
      res.host = std::move(*host);
    }
    if (user) {
      res.user = std::move(*user);
    }
    if (password) {
      res.password = std::move(*password);
    }
    if (auto x = from_string<enum mode>(mode_str.inner)) {
      res.mode = located{*x, mode_str.source};
    } else {
      diagnostic::error(
        "`mode` must be one of `create`, `append` or `create_append`")
        .primary(mode_str, "got `{}`", mode_str.inner)
        .emit(ctx);
      return failure::promise();
    }
    if (primary_selector) {
      auto p = primary_selector->path();
      if (p.size() > 1) {
        diagnostic::error("`primary`, must be a top level field")
          .primary(primary_selector->get_location())
          .emit(ctx);
        return failure::promise();
      }
      res.primary = {p.front().id.name, primary_selector->get_location()};
      if (not validate_identifier(res.primary->inner)) {
        emit_invalid_identifier<true>("primary", res.primary->inner,
                                      res.primary->source, ctx);
        return failure::promise();
      }
    }
    if (res.mode.inner == mode::create and not res.primary) {
      diagnostic::error("mode `create` requires `primary` to be set")
        .primary(mode_str)
        .emit(ctx);
      return failure::promise();
    }
    if (port) {
      if (port->inner <= 0 or port->inner > 65535) {
        diagnostic::error("`port` must be between 1 and 65535")
          .primary(port->source, "got `{}`", port->inner)
          .emit(ctx);
        return failure::promise();
      }
      res.port = {static_cast<uint64_t>(port->inner), port->source};
    }
    auto sp = session_provider::make(ctx);
    if (auto table_name = try_const_eval(res.table, sp.as_session())) {
      if (const auto* s = try_as<std::string>(*table_name)) {
        if (not validate_table_name<true>(*s, res.table.get_location(), ctx)) {
          return failure::promise();
        }
      } else {
        diagnostic::error("`table` must be a `string`")
          .primary(res.table.get_location())
          .emit(ctx);
        return failure::promise();
      }
    }
    return res;
  }

  friend auto inspect(auto& f, operator_arguments& x) -> bool {
    return f.object(x).fields(
      f.field("operator_location", x.operator_location), f.field("uri", x.uri),
      f.field("host", x.host), f.field("port", x.port), f.field("user", x.user),
      f.field("password", x.password), f.field("table", x.table),
      f.field("mode", x.mode), f.field("primary", x.primary),
      f.field("ssl", x.ssl));
  }
};
} // namespace tenzir::plugins::clickhouse

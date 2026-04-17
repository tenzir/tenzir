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
#include "tenzir/tls_options.hpp"
#include "tenzir/tql2/eval.hpp"
#include "tenzir/tql2/plugin.hpp"

#include <boost/regex.hpp>
#include <boost/url/parse.hpp>

namespace tenzir::plugins::clickhouse {

TENZIR_ENUM(mode, create_append, create, append);

constexpr static auto validation_expr = "^[a-zA-Z_][0-9a-zA-Z_]*$";
inline const auto table_name_quoting
  = detail::quoting_escaping_policy{.quotes = "\"`"};

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

template <bool error>
inline auto split_table_name(std::string_view table, location table_loc,
                             diagnostic_handler& dh)
  -> Option<split_table_name_result> {
  const auto dot = table_name_quoting.find_first_of_not_in_quotes(table, ".");
  if (dot == std::string::npos) {
    return split_table_name_result{None{}, table};
  }
  if (dot == table.size() - 1) {
    diag_root<error>("expected table name after `.`")
      .primary(table_loc)
      .emit(dh);
    return None{};
  }
  const auto dot2
    = table_name_quoting.find_first_of_not_in_quotes(table, ".", dot + 1);
  if (dot2 != std::string::npos) {
    diag_root<error>("`table` may contain at most one `.`")
      .note("the `.` separates database and table name")
      .hint("quote the identifiers if you want the `.` to be part of the "
            "identifier")
      .primary(table_loc)
      .emit(dh);
    return None{};
  }
  return split_table_name_result{
    table.substr(0, dot),
    table.substr(dot + 1),
  };
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
    auto port = Option<located<int64_t>>{};
    auto primary_selector = Option<ast::field_path>{};
    auto parser = argument_parser2::operator_(operator_name);
    parser.named_optional("host", res.host);
    parser.named("port", port);
    parser.named_optional("user", res.user);
    parser.named_optional("password", res.password);
    parser.named("table", res.table, "string");
    parser.named_optional("mode", mode_str);
    parser.named("primary", primary_selector, "field");
    res.ssl.add_tls_options(parser);
    TRY(parser.parse(inv, ctx));
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
      f.field("operator_location", x.operator_location),
      f.field("host", x.host), f.field("port", x.port), f.field("user", x.user),
      f.field("password", x.password), f.field("table", x.table),
      f.field("mode", x.mode), f.field("primary", x.primary),
      f.field("ssl", x.ssl));
  }
};
} // namespace tenzir::plugins::clickhouse

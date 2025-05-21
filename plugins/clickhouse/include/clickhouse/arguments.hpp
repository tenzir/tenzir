//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "clickhouse/client.h"
#include "tenzir/argument_parser2.hpp"
#include "tenzir/detail/enum.hpp"
#include "tenzir/ssl_options.hpp"
#include "tenzir/tql2/plugin.hpp"

#include <boost/regex.hpp>
#include <boost/url/parse.hpp>

namespace tenzir::plugins::clickhouse {

TENZIR_ENUM(mode, create_append, create, append);

constexpr static auto validation_expr = "^[a-zA-Z_][0-9a-zA-Z_]*$";

inline auto validate_identifier(std::string_view text) -> bool {
  const static auto quoting = detail::quoting_escaping_policy{.quotes = "\"`"};
  if (quoting.is_quoted(text)) {
    return true;
  }
  const static auto re = boost::regex{validation_expr};
  return boost::regex_match(text.begin(), text.end(), re);
}

inline auto
emit_invalid_identifier(std::string_view name, std::string_view value,
                        location loc, diagnostic_handler& dh) {
  diagnostic::error("invalid {} `{}`", name, value)
    .primary(loc)
    .hint("{} must either be a quoted string, or match the regular "
          "expression `{}`",
          name, validation_expr)
    .emit(dh);
}

struct arguments {
  tenzir::location operator_location;
  located<std::string> host = {"localhost", operator_location};
  located<uint16_t> port = {9000, operator_location};
  located<std::string> user = {"default", operator_location};
  located<std::string> password = {"", operator_location};
  located<std::string> table = {"REQUIRED", location::unknown};
  located<enum mode> mode = located{mode::create_append, operator_location};
  std::optional<located<std::string>> primary = std::nullopt;
  ssl_options ssl = {};

  static auto try_parse(std::string operator_name,
                        operator_factory_plugin::invocation inv, session ctx)
    -> failure_or<arguments> {
    auto res = arguments{inv.self.get_location()};
    auto mode_str = located<std::string>{
      to_string(mode::create_append),
      res.operator_location,
    };
    auto port = std::optional<located<int64_t>>{};
    auto primary_selector = std::optional<ast::field_path>{};
    auto parser = argument_parser2::operator_(operator_name);
    parser.named_optional("host", res.host);
    parser.named("port", port);
    parser.named_optional("user", res.user);
    parser.named_optional("password", res.password);
    parser.named("table", res.table);
    parser.named_optional("mode", mode_str);
    parser.named("primary", primary_selector, "field");
    res.ssl.add_tls_options(parser);
    TRY(parser.parse(inv, ctx));
    if (not validate_identifier(res.table.inner)) {
      emit_invalid_identifier("table", res.table.inner, res.table.source, ctx);
      return failure::promise();
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
    if (res.mode.inner == mode::create and not res.primary) {
      diagnostic::error("mode `create` requires `primary` to be set")
        .primary(mode_str)
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
        emit_invalid_identifier("primary", res.primary->inner,
                                res.primary->source, ctx);
        return failure::promise();
      }
    }
    TRY(res.ssl.validate(res.host, ctx));
    if (not port) {
      if (res.ssl.get_tls().inner) {
        port = located{9440, res.operator_location};
      } else {
        port = located{9000, res.operator_location};
      }
    }
    res.port = *port;
    return res;
  }

  auto make_options() const -> ::clickhouse::ClientOptions {
    auto opts = ::clickhouse::ClientOptions()
                  .SetEndpoints({{host.inner, port.inner}})
                  .SetUser(user.inner)
                  .SetPassword(password.inner);
    if (ssl.get_tls().inner) {
      auto tls_opts = ::clickhouse::ClientOptions::SSLOptions{};
      tls_opts.SetSkipVerification(ssl.skip_peer_verification.has_value());
      auto commands
        = std::vector<::clickhouse::ClientOptions::SSLOptions::CommandAndValue>{};
      if (ssl.cacert) {
        commands.emplace_back("ChainCAFile", ssl.cacert->inner);
      }
      if (ssl.certfile) {
        commands.emplace_back("Certificate", ssl.certfile->inner);
      }
      if (ssl.keyfile) {
        commands.emplace_back("PrivateKey", ssl.keyfile->inner);
      }
      tls_opts.SetConfiguration(commands);
      opts.SetSSLOptions(std::move(tls_opts));
    }
    return opts;
  }

  friend auto inspect(auto& f, arguments& x) -> bool {
    return f.object(x).fields(
      f.field("operator_location", x.operator_location),
      f.field("host", x.host), f.field("port", x.port), f.field("user", x.user),
      f.field("password", x.password), f.field("table", x.table),
      f.field("mode", x.mode), f.field("primary", x.primary),
      f.field("ssl", x.ssl));
  }
};
} // namespace tenzir::plugins::clickhouse

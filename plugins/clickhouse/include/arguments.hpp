//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/tql2/plugin.hpp"
#include "tenzir/detail/enum.hpp"
#include "tenzir/argument_parser2.hpp"

#include <boost/url/parse.hpp>

#include "clickhouse/client.h"

namespace tenzir::plugins::clickhouse {

  TENZIR_ENUM(mode, create_append, create, append);

  struct Arguments {
    tenzir::location operator_location;
    located<std::string> host = {"localhost", operator_location};
    located<uint16_t> port = {9000, operator_location};
    located<std::string> user = {"default", operator_location};
    located<std::string> password = {"", operator_location};
    located<std::string> table = {"REQUIRED", location::unknown};
    located<enum mode> mode = located{mode::create_append, operator_location};
    std::optional<located<std::string>> primary = std::nullopt;

    static auto try_parse(std::string operator_name,
                          operator_factory_plugin::invocation inv,
                          session ctx) -> failure_or<Arguments> {
      auto res = Arguments{inv.self.get_location()};
      auto mode_str = located<std::string>{
        to_string(mode::create_append),
        res.operator_location,
      };
      auto url_str = located<std::string>{
        res.host.inner + ":" + std::to_string(res.port.inner),
        res.operator_location,
      };
      auto primary_selector = std::optional<ast::simple_selector>{};
      auto parser = argument_parser2::operator_(operator_name);
      parser.named_optional("url", url_str);
      parser.named_optional("user", res.user);
      parser.named_optional("password", res.password);
      parser.named("table", res.table);
      parser.named_optional("mode", mode_str);
      parser.named("primary", primary_selector, "field");
      TRY(parser.parse(inv, ctx));
      auto parsed_url = boost::urls::parse_uri(url_str.inner);
      if (not parsed_url) {
        res.host = url_str;
      } else {
        if (parsed_url->has_port()) {
          res.port = {parsed_url->port_number(), url_str.source};
        }
        res.host = {parsed_url->host(), url_str.source};
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
        res.primary = {p.front().name, primary_selector->get_location()};
      }
      return res;
    }

    auto make_options() const -> ::clickhouse::ClientOptions {
      return ::clickhouse::ClientOptions()
                    .SetEndpoints({{host.inner, port.inner}})
                    .SetUser(user.inner)
                    .SetPassword(password.inner);
    }

    friend auto inspect(auto& f, Arguments& x) -> bool {
      return f.object(x).fields(
        f.field("operator_location", x.operator_location),
        f.field("host", x.host), f.field("port", x.port),
        f.field("user", x.user), f.field("password", x.password),
        f.field("table", x.table), f.field("mode", x.mode),
        f.field("primary", x.primary));
    }
  };
}
